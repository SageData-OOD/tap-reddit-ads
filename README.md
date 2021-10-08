# tap-reddit-ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This will extracting data from the [Reddit Ads API](https://ads-api.reddit.com/docs/)

- Extracts the following resources:
  - [Reports](https://ads-api.reddit.com/docs/#tag/Reporting)
  - [Accounts](https://ads-api.reddit.com/docs/#tag/Accounts)
  - [Ads](https://ads-api.reddit.com/docs/#tag/Ads)
  - [Ad_groups](https://ads-api.reddit.com/docs/#tag/Ad-Groups)
  - [Campaigns](https://ads-api.reddit.com/docs/#tag/Campaigns)

## Quick Start

1. Install

    pip install git+https://github.com/SageData-OOD/tap-reddit-ads

2. Create the config file

   Create a JSON file called `config.json`. Its contents should look like:

   ```json
    {
        "starts_at": "2021-09-20",
        "account_id": "<Reddit ads account id>",
        "refresh_token": "<Reddit Oauth refresh token>",
        "client_id": "<Your app client_id>",
        "client_secret": "<Your app client_secret>",
        "user_agent": "tap-reddit-ads <support@your-domain.com>"
    }
    ```

   - The `starts_at` specifies the date at which the tap will begin pulling data
   (Supports in only **Reports** resource).

   - The `account_id` : Reddit ads account id 
     - "URL will have the ad account id you need.
     - `https://ads.reddit.com/account/<AD ACCOUNT ID HERE>/dashboard/campaigns`

   - The `refresh_token`: The reddit OAuth Refresh Token
   - The `client_id`: The reddit OAuth client id
   - The `client_secret`: The reddit OAuth client secret
   - The `user_agent`: tap-reddit-ads <api_user_email@your_company.com>

4. Run the Tap in Discovery Mode

    tap-reddit-ads -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode

    tap-reddit-ads -c config.json --catalog catalog-file.json

---

Copyright &copy; 2021 SageData
