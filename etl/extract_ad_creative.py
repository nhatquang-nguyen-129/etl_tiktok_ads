import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import requests
import pandas as pd

def extract_ad_creative(
    access_token: str,
    advertiser_id: str,   
) -> pd.DataFrame:
    """
    Extract TikTok Ads ad creative
    ---------
    Workflow:
        1. No need input ad_ids 
        2. No need to loop each ad_id
        3. Paginate video creative endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        DataFrame:
            Flattened ad creative records
    """

    start_time = time.time()
    rows: list[dict] = []
    retryable = True

    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }

    timeout = (
        10, 
        600
    )

    # Make TikTok Ads API v1.3 call for ad creative
    video_url = "https://business-api.tiktok.com/open_api/v1.3/file/video/ad/search/"

    page = 1
    page_size = 100
    pagination_continue = True

    try:
        print(
            "🔍 [EXTRACT] Extracting TikTok Ads ad creative for advertiser_id "
            f"{advertiser_id}..."
        )
        
        while pagination_continue:
            payload = {
                "advertiser_id": advertiser_id,
                "page_size": page_size,
                "page": page,
            }

            resp = requests.get(
                video_url,
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
                        "❌ [EXTRACT] Failed to extract TikTok Ads ad creative for advertiser_id "
                        f"{advertiser_id} due to expired or invalid access token then manual token refresh is required."
                    )

        # Unexpected retryable API error
                if code in {
                    40102, 
                    50000, 
                    50001
                }:
                    raise RuntimeError(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads ad creative for advertiser_id "
                        f"{advertiser_id} due to API error "
                        f"{message} with error code "
                        f"{code} then this request is eligible to retry."
                    )

        # Unexpected non-retryable API error
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads ad creative for advertiser_id "
                    f"{advertiser_id} due to API error "
                    f"{message} with error code "                    
                    f"{code} then this request is not eligible to retry."
                )

            video_list = data.get("data", {}).get("list", [])

            for record in video_list:
                rows.append(
                    {
                        "advertiser_id": advertiser_id,
                        "video_id": record.get("video_id"),
                        "video_cover_url": record.get("video_cover_url"),
                        "preview_url": record.get("preview_url"),
                        "create_time": record.get("create_time"),
                    }
                )

            page_info = data.get("data", {}).get("page_info", {})
            total_page = page_info.get("total_page", 1)

            pagination_continue = page < total_page
            page += 1

        print(
            "✅ [EXTRACT] Successfully extracted TikTok Ads ad creative for advertiser_id "
            f"{advertiser_id} with "
            f"{len(rows)} record(s)."           
        )

    except requests.HTTPError as e:
        status = e.response.status_code if e.response else None

        # Unexpected retryable HTTP request error 
        if status and status >= 500:
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads ad creative for advertiser_id "
                f"{advertiser_id} due to "
                f"{e} with HTTP request status "
                f"{status} then this request is eligible to retry."
            ) from e

        # Unexpected non-retryable HTTP request error
        raise RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads ad creative for advertiser_id "
                f"{advertiser_id} due to "
                f"{e} with HTTP request status "
                f"{status} then this request is not eligible to retry."
            ) from e
    
        # Unknown non-retryable error     
    except Exception as e:
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads ad creative for advertiser_id "
            f"{advertiser_id} due to "
            f"{e}."
        ) from e

    df = pd.DataFrame(rows)
    df.attrs["retryable"] = retryable
    df.attrs["time_elapsed"] = round(time.time() - start_time, 2)
    df.attrs["rows_input"] = None
    df.attrs["rows_output"] = len(df)

    return df