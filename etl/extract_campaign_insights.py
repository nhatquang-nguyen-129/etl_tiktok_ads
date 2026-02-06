import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import requests
import pandas as pd

def extract_campaign_insights(
    advertiser_id: str,
    access_token: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Extract TikTok Ads campaign insights
    ---------
    Workflow:
        1. Validate input advertiser_id
        2. Validate input start_date and end_date
        3. Make API call for report/integrated/get endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        1. DataFrame:
            Flattened campaign insights records
    """

    start_time = time.time()

    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
    
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json"
    }

    dimensions = [
        "campaign_id",
        "stat_time_day",
    ]

    metrics = [
        "result",
        "spend",
        "impressions",
        "clicks",
        "engaged_view_15s",
        "purchase",
        "complete_payment",
        "onsite_total_purchase",
        "offline_shopping_events",
        "onsite_shopping",
        "messaging_total_conversation_tiktok_direct_message",
    ]

    payload = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_CAMPAIGN",
        "dimensions": dimensions,
        "metrics": metrics,
        "start_date": start_date,
        "end_date": end_date,
        "page_size": 1000,
        "page": 1
    }

    print(
        "🔍 [EXTRACT] Extracting TikTok Ads campaign insights for advertiser_id "
        f"{advertiser_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )

    records = []

    # Make TikTok Ads API v1.3 call for campaign insights
    try:
        while True:
            resp = requests.get(
                url,
                headers=headers,
                json=payload,
                timeout=60
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
                    retryable = False
                    raise RuntimeError(
                        "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                        f"{advertiser_id} from "
                        f"{start_date} to "
                        f"{end_date} due to expired or invalid access token then manual token refresh is required."
                    )

        # Unexpected retryable API error
                if code in {
                    40102, 
                    50000, 
                    50001
                }:
                    retryable = True
                    raise RuntimeError(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                        f"{advertiser_id} from "
                        f"{start_date} to "
                        f"{end_date} due to API error "
                        f"{message} with error code "
                        f"{code} then this request is eligible to retry."
                    )

        # Unexpected non-retryable API error
                retryable = False
                raise RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                    f"{advertiser_id} from "
                    f"{start_date} to "
                    f"{end_date} due to API error "
                    f"{message} with error code "
                    f"{code} then this request is not eligible to retry."
                )

            batch = data.get("data", {}).get("list", [])
            records.extend(batch)

            if len(batch) < payload["page_size"]:
                break

            payload["page"] += 1

        rows = []
        for record in records:
            row = {}
            row.update(record.get("dimensions", {}))
            row.update(record.get("metrics", {}))
            row["advertiser_id"] = advertiser_id
            rows.append(row)

        df = pd.DataFrame(rows)
        df.retryable = False
        df.time_elapsed = round(time.time() - start_time, 2)
        df.rows_input = None
        df.rows_output = len(df)

        return df

        # Unexpected retryable request timeout error
    except requests.exceptions.Timeout as e:      
        retryable = True
        raise RuntimeError(
            "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to request timeout error then this request is eligible to retry."
        ) from e

        # Unexpected retryable request connection timeout error
    except requests.exceptions.ConnectionError as e:
        retryable = True
        raise RuntimeError(
            "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to request connection error then this request is eligible to retry."
        ) from e

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else None
        
        # Unexpected retryable HTTP request error   
        if status and status >= 500:
            retryable = True
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                f"{advertiser_id} from "
                f"{start_date} to "
                f"{end_date} due to "
                f"{e} with HTTP request status "
                f"{status} then this request is eligible to retry."
            ) from e

        # Unexpected non-retryable HTTP request error
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to "
            f"{e} with HTTP request status "
            f"{status} then this request is not eligible to retry."
        ) from e

        # Unknown non-retryable error 
    except Exception as e:
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to "
            f"{e}."
        ) from e