import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import requests
import pandas as pd

def extract_ad_metadata(
    access_token: str,
    advertiser_id: str,    
    ad_ids: list[str],
) -> pd.DataFrame:
    """
    Extract TikTok Ads ad metadata
    ---------
    Workflow:
        1. Validate input ad_ids
        2. Make API call for v1.3/advertiser/info
        2. Make API call for v1.3/ad/get
        3. Append extracted JSON data to list[dict]
        4. Enforce List[dict] to DataFrame
    ---------
    Returns:
        DataFrame:
            Flattened ad metadata records
    """

    start_time = time.time()
    rows: list[dict] = []
    failed_ad_ids: list[str] = []
    retryable = True

    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }

    timeout =(
        10,
        600
    )

    # Validate input
    if not ad_ids:
        df = pd.DataFrame(
            columns=[
                "advertiser_id",
                "advertiser_name",
                "ad_id",
                "ad_name",
                "adgroup_id",
                "adgroup_name",
                "campaign_id",
                "campaign_name",
                "operation_status",
                "ad_format",
                "optimization_event",
                "video_id",
            ]
        )
        df.attrs("failed_ad_ids") = []
        df.attrs("retryable") = False
        df.attrs("time_elapsed") = round(time.time() - start_time, 2)
        df.attrs("rows_input") = 0
        df.attrs("rows_output") = 0
        return df

    # Make TikTok Ads API v1.3 call for advertiser name
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
            timeout=timeout,
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

        advertiser_name = data["data"]["list"][0].get("name")

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

    # Make TikTok Ads API v1.3 call for ad metadata
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

        # Expired token error
                if code in {
                    40100, 
                    40101
                }:
                    raise RuntimeError(
                        "❌ [EXTRACT] Failed to extract TikTok Ads ad metadata for advertiser_id "
                        f"{advertiser_id} due to expired or invalid access token then manual token refresh is required."
                    )

        # Unexpected retryable API error
                if code in {
                    40102, 
                    50000, 
                    50001
                }:
                    
                    failed_ad_ids.append(ad_id)

                    print(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                        f"{ad_id} due to API error "
                        f"{message} with error code "
                        f"{code} then request for this ad_id is eligible to retry."
                    )

                    rows.append(
                        {
                            "advertiser_id": advertiser_id,
                            "advertiser_name": advertiser_name,
                            "ad_id": ad_id,
                            "ad_name": None,
                            "adgroup_id": None,
                            "adgroup_name": None,
                            "campaign_id": None,
                            "campaign_name": None,
                            "operation_status": None,
                            "create_time": None,
                            "ad_format": None,
                            "optimization_event": None,
                            "video_id": None,
                        }
                    )
                    continue

        # Unexpected non-retryable API error
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads ad metadata for ad_id "
                    f"{ad_id} due to API error "
                    f"{message} with with error code "
                    f"{code} then request for this ad_id is not eligible to retry."
                )
            
            ad_list = data.get("data", {}).get("list", [])

            if ad_list:
                ad = ad_list[0]
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
            else:
                rows.append(
                    {
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                        "ad_id": ad_id,
                        "ad_name": None,
                        "adgroup_id": None,
                        "adgroup_name": None,
                        "campaign_id": None,
                        "campaign_name": None,
                        "operation_status": None,
                        "create_time": None,
                        "ad_format": None,
                        "optimization_event": None,
                        "video_id": None,
                    }
                )

        except requests.HTTPError as e:
            status = e.response.status_code if e.response else None

        # Unexpected retryable HTTP request error            
            if status and status >= 500:
                failed_ad_ids.append(ad_id)

                print(
                    "⚠️ [EXTRACT] Failed to extract TikTok ad metadata for ad_id "
                    f"{ad_id} due to "
                    f"{e} with HTTP request status "
                    f"{status} then request for this ad_id is eligible to retry."
                )

                rows.append(
                    {
                        "advertiser_id": advertiser_id,
                        "advertiser_name": advertiser_name,
                        "ad_id": ad_id,
                        "ad_name": None,
                        "adgroup_id": None,
                        "adgroup_name": None,
                        "campaign_id": None,
                        "campaign_name": None,
                        "operation_status": None,
                        "create_time": None,
                        "ad_format": None,
                        "optimization_event": None,
                        "video_id": None,
                    }
                )
                continue

        # Unexpected non-retryable HTTP request error
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok ad metadata for ad_id "
                f"{ad_id} due to "
                f"{e} with HTTP request status "
                f"{status} then request for this ad_id is not eligible to retry."                
            ) from e

        # Unknown non-retryable error 
        except Exception as e:
            raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok ad metadata for ad_id "
                f"{ad_id} due to "
                f"{e}."
            ) from e

    df = pd.DataFrame(rows)
    df.attrs("failed_ad_ids") = failed_ad_ids
    df.attrs("retryable") = bool(failed_ad_ids) and retryable
    df.attrs("time_elapsed") = round(time.time() - start_time, 2)
    df.attrs("rows_input") = len(ad_ids)
    df.attrs("rows_output") = len(df)

    return df