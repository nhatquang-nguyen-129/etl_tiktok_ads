import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
import requests
import pandas as pd

def extract_campaign_metadata(
    advertiser_id: str,
    access_token: str,
    campaign_id: str,
) -> pd.DataFrame:
    """
    Extract TikTok Ads campaign metadata
    ---------
    Workflow:
        1. Fetch advertiser name
        2. Fetch campaign metadata
        3. Append extracted JSON data to list[dict]
        4. Enforce List[dict] to DataFrame
    ---------
    Returns:
        DataFrame:
            Flattened campaign metadata records
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_campaign_ids: list[str] = []
    retryable = True

    if not campaign_id:
        df = pd.DataFrame(
            columns=[
                "campaign_id",
                "campaign_name",
                "status",
                "advertiser_id",
                "advertiser_name",
            ]
        )
        df.failed_campaign_ids = []
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = 0
        df.rows_output = 0
        return df

    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }

    # Make API call to advertiser information endpoint
    try:
        advertiser_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
        advertiser_payload = {"advertiser_ids": [advertiser_id]}

        resp = requests.get(
            advertiser_url,
            headers=headers,
            json=advertiser_payload,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            code = data.get("code")
            message = data.get("message")

            # Expired token error
            if code in {
                40100, 
                40101
            }:
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                    f"{advertiser_id} due to expired or invalid access token then manual token refresh is required."
                )

            # Unexpected retryable error
            if code in {
                40102, 
                50000, 
                50001
            }:
                raise RuntimeError(
                    "⚠️ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                    f"{advertiser_id} due to "
                    f"{message} with API request error code "
                    f"{code} then this API call is eligible to retry."
                )

            # Unexpected non-retryable error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                    f"{advertiser_id} due to "
                    f"{message} with unexpected API request error code "
                    f"{code} then this API call is not eligible to retry."
                )

        advertiser_name = data["data"]["list"][0].get("advertiser_name")

    except requests.HTTPError as e:
        status = e.response.status_code if e.response else None

        # Unexpected retryable HTTP request error
        if status and status >= 500:
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok advertiser_name for advertiser_id "
                f"{advertiser_id} due to "
                f"{e} with HTTP request status "
                f"{status} then this API call is eligible to retry."
            ) from e

        # Unexpected non-retryable HTTP request error
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok advertiser_name for advertiser_id "
            f"{advertiser_id} due to "
            f"{e} with HTTP request status "
            f"{status} then this API call is not eligible to retry."
        ) from e

    except Exception as e:
        # Unknown non-retryable error        
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok advertiser info for advertiser_id "
            f"{advertiser_id} due to "
            f"{e}."
        ) from e

    # Make API call to campaign metadata endpoint
    try:
        campaign_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
        campaign_payload = {
            "advertiser_id": advertiser_id,
            "filtering": {"campaign_ids": [campaign_id]},
            "fields": [
                "campaign_id",
                "campaign_name",
                "status",
            ],
        }

        resp = requests.get(
            campaign_url,
            headers=headers,
            json=campaign_payload,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            code = data.get("code")
            message = data.get("message")

            # Token expired / invalid
            if code in {40100, 40101}:
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok campaign metadata "
                    "due to token expired or invalid then manual token refresh is required."
                )

            # Retryable API error
            if code in {40102, 50000, 50001}:
                failed_campaign_ids.append(campaign_id)

                msg = (
                    "⚠️ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                    f"{campaign_id} due to API error {code} - {message} "
                    "then this campaign_id is eligible to retry."
                )
                print(msg)
                logging.warning(msg)

                rows.append(
                    {
                        "campaign_id": campaign_id,
                        "campaign_name": None,
                        "status": None,
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                    }
                )

            else:
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                    f"{campaign_id} due to unexpected API error {code} - {message}."
                )

        else:
            campaign_list = data.get("data", {}).get("list", [])

            if campaign_list:
                campaign = campaign_list[0]
                rows.append(
                    {
                        "campaign_id": campaign.get("campaign_id"),
                        "campaign_name": campaign.get("campaign_name"),
                        "status": campaign.get("status"),
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                    }
                )
            else:
                rows.append(
                    {
                        "campaign_id": campaign_id,
                        "campaign_name": None,
                        "status": None,
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                    }
                )

    except requests.HTTPError as e:
        status = e.response.status_code if e.response else None

        # Retryable HTTP error
        if status and status >= 500:
            failed_campaign_ids.append(campaign_id)

            msg = (
                "⚠️ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                f"{campaign_id} due to HTTP {status} then this campaign_id is eligible to retry."
            )
            print(msg)
            logging.warning(msg)

            rows.append(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": None,
                    "status": None,
                    "advertiser_id": advertiser_id,
                    "advertiser_name": advertiser_name,
                }
            )

        else:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                f"{campaign_id} due to HTTP error {e}."
            ) from e

    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
            f"{campaign_id} due to {e}."
        ) from e

    # =====================================================
    # 3. Finalize DataFrame
    # =====================================================
    df = pd.DataFrame(rows)
    df.failed_campaign_ids = failed_campaign_ids
    df.retryable = bool(failed_campaign_ids) and retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = 1
    df.rows_output = len(df)

    return df
