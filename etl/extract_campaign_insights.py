import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import time
import logging
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
        3. Make API call to TikTok integrated report endpoint
        4. Append extracted JSON data to list[dict]
        5. Flatten dimensions and metrics to DataFrame
    ---------
    Returns:
        DataFrame:
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

    msg = (
        "🔍 [EXTRACT] Extracting TikTok Ads campaign insights for advertiser_id "
        f"{advertiser_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )
    print(msg)
    logging.info(msg)

    try:
        records = []

        while True:
            response = requests.get(
                url,
                headers=headers,
                json=payload,
                timeout=60
            )

            response.raise_for_status()
            result = response.json()

            # --- TikTok API-level error ---
            if result.get("code") != 0:
                api_code = result.get("code")
                api_message = result.get("message", "Unknown TikTok API error")
                raise RuntimeError(
                    f"TikTok API error | code={api_code} | message={api_message}"
                )

            batch = result.get("data", {}).get("list", [])
            records.extend(batch)

            if len(batch) < payload["page_size"]:
                break

            payload["page"] += 1

        # --- Flatten ---
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

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else None

        # TikTok server-side / throttling → retryable
        if status_code and status_code >= 500:
            retryable = True
            raise RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                f"{advertiser_id} from "
                f"{start_date} to "
                f"{end_date} due to server-side HTTP error then this request is eligible to retry."
            ) from e

        # Client-side HTTP error → non-retryable
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to HTTP error "
            f"{status_code} then this request is not eligible to retry."
        ) from e

    except RuntimeError as e:
        # TikTok API logical error
        retryable = True
        raise RuntimeError(
            "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to TikTok API error then this request is eligible to retry."
        ) from e

    except Exception as e:
        retryable = False
        raise RuntimeError(
            "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} due to unknown error "
            f"{e}."
        ) from e