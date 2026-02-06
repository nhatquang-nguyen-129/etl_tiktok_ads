import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from typing import List
import time
import requests
import pandas as pd

def extract_campaign_metadata(
    advertiser_id: str,
    access_token: str,
    campaign_ids: List[str],
) -> pd.DataFrame:
    """
    Extract TikTok Ads campaign metadata
    ---------
    Workflow:
        1. Validate input campaign_ids
        2. Make API call for v1.3/advertiser/info
        2. Make API call for v1.3/campaign/get
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

    # Validate input
    if not campaign_ids:
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

    # Make TikTok Ads API v1.3 call for advertiser name
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }
    
    try:
        print(
            "🔍 [EXTRACT] Extracting TikTok Ads advertiser_name for advertiser_id "
            f"{advertiser_id}..."
        )

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

        # Unexpected retryable API error
            if code in {
                40102, 
                50000, 
                50001
            }:
                raise RuntimeError(
                    "⚠️ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                    f"{advertiser_id} due to API error "
                    f"{message} with error code "
                    f"{code} then this request is eligible to retry."
                )

        # Unexpected non-retryable API error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                f"{advertiser_id} due to API error "
                f"{message} with error code "
                f"{code} then this request is not eligible to retry."
            )

        advertiser_name = data["data"]["list"][0].get("advertiser_name")

        print(
            "✅ [EXTRACT] Successfully extracted TikTok Ads advertiser_name "
            f"{advertiser_name} for advertiser_id "
            f"{advertiser_id}."
        )

    except requests.HTTPError as e:
        status = e.response.status_code if e.response else None
        
        # Unexpected retryable HTTP request error
        if status and status >= 500:
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                f"{advertiser_id} due to "
                f"{e} with HTTP request status "
                f"{status} then this request is eligible to retry."
            ) from e

        # Unexpected non-retryable HTTP request error
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
            f"{advertiser_id} due to "
            f"{e} with HTTP request status "
            f"{status} then this request is not eligible to retry."
        ) from e

        # Unknown non-retryable error
    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads advertiser info for advertiser_id "
            f"{advertiser_id} due to "
            f"{e}."
        ) from e

    # Make TikTok Ads API v1.3 call for campaign metadata
    campaign_metadata_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
    
    fields = [
        "campaign_id",
        "campaign_name",
        "status",
        "objective_type",
        "advertiser_id",
        "advertiser_name",
    ]

    print(
        "🔍 [EXTRACT] Extracting TikTok Ads campaign metadata for advertiser_id "
        f"{advertiser_id} with "
        f"{len(campaign_ids)} campaign_id(s)..."
    )

    for campaign_id in campaign_ids:
        try:
            payload = {
                "advertiser_id": advertiser_id,
                "filtering": {"campaign_ids": [campaign_id]},
                "fields": fields,
            }            

            resp = requests.get(
                campaign_metadata_url,
                headers=headers,
                json=payload,
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
                        "❌ [EXTRACT] Failed to extract TikTok Ads campaign metadata for advertiser_id "
                        f"{advertiser_id} due to expired or invalid access token then manual token refresh is required."
                    )

        # Unexpected retryable API error
                if code in {
                    40102, 
                    50000, 
                    50001
                }:
                    
                    failed_campaign_ids.append(campaign_id)

                    print(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                        f"{campaign_id} due to API error "
                        f"{message} with error code "
                        f"{code} then request for this campaign_id is eligible to retry."
                    )

                    rows.append(
                        {
                            "campaign_id": campaign_id,
                            "campaign_name": None,
                            "status": None,
                            "advertiser_id": advertiser_id,
                            "advertiser_name": advertiser_name,
                        }
                    )
                    continue

        # Unexpected non-retryable API error
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                    f"{campaign_id} due to API error"
                    f"{message} with error code "
                    f"{code} then request for this campaign_id is not eligible to retry."
                )

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

        # Unexpected retryable HTTP request error
            if status and status >= 500:
                failed_campaign_ids.append(campaign_id)

                print(
                    "⚠️ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                    f"{campaign_id} due to "
                    f"{e} with HTTP request status "
                    f"{status} then request for this campaign_id is eligible to retry."
                )

                rows.append(
                    {
                        "campaign_id": campaign_id,
                        "campaign_name": None,
                        "status": None,
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                    }
                )
                continue

        # Unexpected non-retryable HTTP request error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                f"{campaign_id} due to "
                f"{e} with HTTP request status "
                f"{status} then this request for this campaign_id is not eligible to retry."
            ) from e

        # Unknown non-retryable error 
        except Exception as e:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok campaign metadata for campaign_id "
                f"{campaign_id} due to "
                f"{e}."
            ) from e

    df = pd.DataFrame(rows)
    df.failed_campaign_ids = failed_campaign_ids
    df.retryable = bool(failed_campaign_ids) and retryable
    df.time_elapsed = round(time.time() - start_time, 2)
    df.rows_input = len(campaign_ids)
    df.rows_output = len(df)

    return df