import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import requests
import pandas as pd

def extract_ad_metadata(
    access_token: str,
    advertiser_id: str,    
    ad_ids: list[str],
) -> pd.DataFrame:
    """
    Extract TikTok Ads ad metadata
    ---
    Principles:
        1. Validate input ad_ids
        2. Make API call for v1.3/advertiser/info
        3. Make API call for v1.3/ad/get
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---
    Returns:
        DataFrame:
            Flattened ad metadata records
    """

    # Validate input
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }

    timeout = (
        30, 
        600
    )

    rows: list[dict] = []
    
    ad_id_has_retryable_error = False

    if not ad_ids:

        print(
            "⚠️ [EXTRACT] No input ad_ids for TikTok Ads advertiser_id "
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

        # Unknown non-retryable error   
    except Exception as e:
     
        error = RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads advertiser_name for advertiser_id "
            f"{advertiser_id} due to "
            f"{e}."
        )

        error.retryable = False

        raise error from e

    # Make TikTok Ads API call for ad metadata
    ad_metadata_url = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"

    fields = [
        "ad_id",
        "ad_name",
        "adgroup_id",
        "adgroup_name",
        "campaign_id",
        "campaign_name",
        "operation_status",
        "create_time",
        "ad_format",
        "optimization_event",
        "video_id",
    ]

    print(
        "🔍 [EXTRACT] Extracting TikTok Ads ad metadata for advertiser_id "
        f"{advertiser_id} with "
        f"{len(ad_ids)} ad_id(s)..."
    )

    for ad_id in ad_ids:

        try:

            payload = {
                "advertiser_id": advertiser_id,
                "filtering": {"ad_ids": [ad_id]},
                "fields": fields,
            }

            resp = requests.get(
                ad_metadata_url,
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
                        "❌ [EXTRACT] Failed to extract TikTok Ads ad metadata for advertiser_id "
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
                    
                    ad_id_has_retryable_error = True

                    print(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                        f"{ad_id} due to API error "
                        f"{message} with error code "
                        f"{code} then this request is eligible to retry."
                    )

                    continue

        # Non-retryable API error
                error = RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                    f"{ad_id} due to API error "
                    f"{message} with error code "
                    f"{code} then this request is not eligible to retry."
                )
                
                error.retryable = False
                
                raise error
            
            block = data.get("data") or {}
            
            batch = block.get("list", [])

            if batch:

                ad = batch[0]

                rows.append(
                    {
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                        "ad_id": ad.get("ad_id"),
                        "ad_name": ad.get("ad_name"),
                        "adgroup_id": ad.get("adgroup_id"),
                        "adgroup_name": ad.get("adgroup_name"),
                        "campaign_id": ad.get("campaign_id"),
                        "campaign_name": ad.get("campaign_name"),
                        "operation_status": ad.get("operation_status"),
                        "create_time": ad.get("create_time"),
                        "ad_format": ad.get("ad_format"),
                        "optimization_event": ad.get("optimization_event"),
                        "video_id": ad.get("video_id"),
                    }
                )

        except requests.HTTPError as e:
            
            status = e.response.status_code if e.response else None

        # Retryable HTTP request error 
            if status and status >= 500:

                ad_id_has_retryable_error = True

                print(
                    "⚠️ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                    f"{ad_id} due to HTTP error "
                    f"{status} then this request is eligible to retry."
                )

                continue

        # Non-retryable HTTP request error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                f"{ad_id} due to HTTP error "
                f"{status} then this request is not eligible to retry."
            )

            error.retryable = False

            raise error from e

        # Unknown non-retryable error
        except Exception as e:
        
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                f"{ad_id} due to "
                f"{e}."
            )

            error.retryable = False

            raise error from e

    df = pd.DataFrame(rows)

    if ad_id_has_retryable_error:

        print(
            "⚠️ [EXTRACT] Partially extracted TikTok Ads ad metadata with "
            f"{len(df)}/{len(ad_ids)} row(s) for advertiser_id "
            f"{advertiser_id}."
        )

    else:

        print(
            "✅ [EXTRACT] Successfully extracted TikTok Ads ad metadata with "
            f"{len(df)}/{len(ad_ids)} row(s) for advertiser_id "
            f"{advertiser_id}."
        )

    return df