#!/usr/bin/env python3
import os
import json
import backoff
import requests
import singer
from datetime import datetime, timedelta
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

REQUIRED_CONFIG_KEYS = ["starts_at", "account_id", "refresh_token", "client_id", "client_secret", "user_agent"]
LOGGER = singer.get_logger()
HOST = "https://ads-api.reddit.com"
PATH = "/api/v2.0/accounts/{account_id}"
DEFAULT_CONVERSION_WINDOW = 14
END_POINTS = {
    "ads_reports": "/reports",
    "ads": "/ads",
    "campaigns": "/campaigns",
    "ad_groups": "/ad_groups",
    "accounts": ""
}


class RedditRateLimitError(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def _refresh_token(config):
    headers = {'User-Agent': config['user_agent']}
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': config['refresh_token']
    }
    url = 'https://www.reddit.com/api/v1/access_token'
    response = requests.post(url, headers=headers, data=data,
                             auth=(config["client_id"], config['client_secret']))
    return response.json()


def refresh_access_token_if_expired(config):
    # if [expires_at not exist] or if [exist and less then current time] then it will update the token
    if config.get('expires_at') is None or config.get('expires_at') < datetime.utcnow():
        res = _refresh_token(config)
        config["access_token"] = res["access_token"]
        config["refresh_token"] = res["refresh_token"]
        config["expires_at"] = datetime.utcnow() + timedelta(seconds=res["expires_in"])
        return True
    return False


def get_key_properties(stream_id):
    if stream_id == "ads_reports":
        return ["date", "account_id", "campaign_id", "ad_group_id", "ad_id"]
    else:
        return ["id"]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def create_metadata_for_report(stream_id, schema, key_properties):
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "FULL_TABLE"}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if stream_id == "ads_reports":
        mdata[0]["metadata"]["forced-replication-method"] = "INCREMENTAL"
        mdata[0]["metadata"]["valid-replication-keys"] = ["date"]

    for key in schema.properties:
        # hence when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop], "metadata": {"inclusion": inclusion}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


@backoff.on_exception(backoff.expo, RedditRateLimitError, max_tries=5, factor=2)
@utils.ratelimit(1, 1)
def request_data(config, attr, headers, endpoint):
    url = HOST + PATH.format(account_id=config["account_id"]) + endpoint
    if attr:
        url += "?" + "&".join([f"{k}={v}" for k, v in attr.items()])

    if refresh_access_token_if_expired(config) or not headers:
        headers.update({'Authorization': f'bearer {config["access_token"]}'})

    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        raise RedditRateLimitError(response.text)
    elif response.status_code != 200:
        raise Exception(response.text)
    data = response.json().get("data", [])

    return [data] if isinstance(data, dict) else data


def get_valid_start_date(date_to_poll, conversion_window):
    """
    fix for data freshness
    e.g. Sunday's data is available at 3 AM UTC on Monday
    If integration is set to sync at 1AM then a problem occurs
    """

    utcnow = datetime.utcnow()
    date_to_poll = datetime.strptime(date_to_poll, "%Y-%m-%d")

    if date_to_poll >= utcnow - timedelta(days=conversion_window):
        date_to_poll = utcnow - timedelta(days=conversion_window)

    return date_to_poll.strftime("%Y-%m-%d")


def sync_reports(config, state, stream):
    bookmark_column = "date"
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    endpoint = END_POINTS[stream.tap_stream_id]
    headers = dict()
    attr = dict()
    starts_at = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column).split(" ")[0] \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) else config["starts_at"]
    conversion_window = config.get("conversion_window", DEFAULT_CONVERSION_WINDOW)
    starts_at = get_valid_start_date(starts_at, conversion_window)

    while True:
        attr["starts_at"] = attr["ends_at"] = starts_at
        LOGGER.info("Querying Date --> %s", attr["starts_at"])
        tap_data = request_data(config, attr, headers, endpoint)

        bookmark = attr["starts_at"]
        with singer.metrics.record_counter(stream.tap_stream_id) as counter:
            for row in tap_data:
                # Type Conversation and Transformation
                transformed_data = transform(row, schema, metadata=mdata)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [transformed_data])
                counter.increment()
                bookmark = max([bookmark, row[bookmark_column]])

        # if there is data, then only we will print state
        if len(tap_data):
            state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, bookmark)
            singer.write_state(state)

        if starts_at < str(datetime.utcnow().date()):
            starts_at = str(datetime.strptime(starts_at, '%Y-%m-%d').date() + timedelta(days=1))
        if bookmark >= str(datetime.utcnow().date()):
            break


def sync_endpoints(config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    endpoint = END_POINTS[stream.tap_stream_id]
    tap_data = request_data(config, {}, {}, endpoint)

    with singer.metrics.record_counter(stream.tap_stream_id) as counter:
        for row in tap_data:
            # Type Conversation and Transformation
            transformed_data = transform(row, schema, metadata=mdata)

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [transformed_data])
            counter.increment()


def sync(config, state, catalog):
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if stream.tap_stream_id == "ads_reports":
            sync_reports(config, state, stream)
        else:
            sync_endpoints(config, state, stream)
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()


