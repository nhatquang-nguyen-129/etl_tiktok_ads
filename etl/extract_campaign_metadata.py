import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import requests
import pandas as pd

def extract_campaign_metadata(
    access_token: str,
    advertiser_id: str,
    campaign_ids: list[str],
) -> pd.DataFrame:
    """
    Extract TikTok Ads campaign metadata
    ---
    Principles:
        1. Validate input campaign_ids
        2. Make API call for v1.3/advertiser/info
        3. Make API call for v1.3/campaign/get
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---
    Returns:
        DataFrame:
            Flattened campaign metadata records
    """

    # Validate input
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }

    timeout = (
        30,
        600,
    )

    rows: list[dict] = []

    campaign_id_has_retryable_error = False

    if not campaign_ids:

        print(
            "⚠️ [EXTRACT] No input campaign_ids for TikTok Ads advertiser_id "
            f"{advertiser_id} then extraction will be suspended."
        )

        return pd.DataFrame()

    # Make TikTok Ads API call for advertiser_name
    advertiser_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"

    payload = {"advertiser_ids": [advertiser_id]}

    try:

        print(
            "🔍 [EXTRACT] Extracting TikTok Ads advertiser_name for advertiser_id "
            f"{advertiser_id}..."
        )

        resp = requests.get(
            advertiser_url,
            headers=headers,
            json=payload,
            timeout=timeout,
        )

        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:

            code = data.get("code")
            message = data.get("message")

        # Expired token
            if code in {
                40100, 
                40101
            }:
                error = RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                    f"{advertiser_id} due to expired or invalid access token."
                )
                error.retryable = False
                raise error

        # Retryable API error
            if code in {
                40102, 
                50000, 
                50001
            }:
                error = RuntimeError(
                    "⚠️ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                    f"{advertiser_id} due to API error "
                    f"{message} with error code "
                    f"{code} then this request is eligible to retry."
                )
                error.retryable = True
                raise error

        # Non-retryable API error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                f"{advertiser_id} due to API error "
                f"{message} with error code "
                f"{code} then this request is not eligible to retry."
            )
            error.retryable = False
            raise error

        advertiser_name = data["data"]["list"][0].get("name")

        print(
            "✅ [EXTRACT] Successfully extracted TikTok Ads advertiser_name "
            f"{advertiser_name} for advertiser_id "
            f"{advertiser_id}."
        )

    except requests.HTTPError as e:

        status = e.response.status_code if e.response else None

        # Retryable HTTP request error
        if status and status >= 500:
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
                f"{advertiser_id} due to HTTP error "
                f"{status} then this request is eligible to retry."
            )
            error.retryable = True
            raise error from e

        # Non-retryable HTTP request error
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
            f"{advertiser_id} due to HTTP error "
            f"{status} then this request is not eligible to retry."
        )
        error.retryable = False
        raise error from e

    except Exception as e:

        # Unknown non-retryable error
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
            f"{advertiser_id} due to "
            f"{e}."
        )
        error.retryable = False
        raise error from e

    # Make TikTok Ads API call for campaign metadata
    campaign_metadata_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"

    fields = [
        "advertiser_id",
        "campaign_id",
        "campaign_name",
        "campaign_type",
        "operation_status",
        "objective",
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
                timeout=timeout,
            )

            resp.raise_for_status()
            data = resp.json()

            if data.get("code") != 0:

                code = data.get("code")
                message = data.get("message")

        # Expired token
                if code in {
                    40100, 
                    40101
                }:
                    error = RuntimeError(
                        "❌ [EXTRACT] Failed to extract TikTok Ads campaign metadata for advertiser_id "
                        f"{advertiser_id} due to expired or invalid access token."
                    )
                    error.retryable = False
                    raise error

        # Retryable API error
                if code in {
                    40102,
                    50000,
                    50001
                }:

                    campaign_id_has_retryable_error = True

                    print(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                        f"{campaign_id} due to API error "
                        f"{message} with error code "
                        f"{code} then this request is eligible to retry."
                    )

                    continue

        # Non-retryable API error
                error = RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                    f"{campaign_id} due to API error "
                    f"{message} with error code "
                    f"{code} then this request is not eligible to retry."
                )
                error.retryable = False
                raise error

            block = data.get("data") or {}
            batch = block.get("list", [])

            if batch:

                campaign = batch[0]

                rows.append(
                    {
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                        "campaign_id": campaign.get("campaign_id"),
                        "campaign_name": campaign.get("campaign_name"),
                        "operation_status": campaign.get("operation_status"),
                        "objective": campaign.get("objective"),
                    }
                )

        except requests.HTTPError as e:

            status = e.response.status_code if e.response else None

        # Retryable HTTP request error 
            if status and status >= 500:

                campaign_id_has_retryable_error = True

                print(
                    "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                    f"{campaign_id} due to HTTP error "
                    f"{status} then this request is eligible to retry."
                )

                continue

        # Non-retryable HTTP request error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                f"{campaign_id} due to HTTP error "
                f"{status} then this request is not eligible to retry."
            )
            error.retryable = False
            raise error from e

        except Exception as e:

        # Unknown non-retryable error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads campaign metadata for campaign_id "
                f"{campaign_id} due to "
                f"{e}."
            )
            error.retryable = False
            raise error from e

    if campaign_id_has_retryable_error:

        error = RuntimeError(
            "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign metadata for "
            f"{len(campaign_ids)} campaign_id(s) from advertiser_id "
            f"{advertiser_id} due to retryable error(s) then this request is eligible to retry."
        )
        error.retryable = True
        raise error

    df = pd.DataFrame(rows)

    print(
        "✅ [EXTRACT] Successfully extracted "
        f"{len(df)}/{len(campaign_ids)} row(s) of TikTok Ads campaign metadata for advertiser_id "
        f"{advertiser_id}."
    )

    return df