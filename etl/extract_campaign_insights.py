import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import requests
import pandas as pd

def extract_campaign_insights(
    access_token: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Extract TikTok Ads campaign insights
    ---------
    Principles:
        1. Validate input advertiser_id
        2. Validate input start_date and end_date
        3. Make API call for report/integrated/get endpoint
        4. Append extracted JSON data to list[dict]
        5. Enforce List[dict] to DataFrame
    ---------
    Returns:
        DataFrame:
            Flattened campaign insights records
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

    campaign_insights_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"

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
        "page": 1,
    }

    records = []

    # Make TikTok Ads API call for campaign insights
    print(
        "🔍 [EXTRACT] Extracting TikTok Ads campaign insights for advertiser_id "
        f"{advertiser_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )

    while True:

        try:
            resp = requests.get(
                campaign_insights_url,
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
                    40101,
                }:
                    error = RuntimeError(
                        "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                        f"{advertiser_id} due to expired or invalid access token."
                    )
                    error.retryable = False
                    raise error

        # Retryable API error
                if code in {
                    40102,
                    50000,
                    50001,
                }:
                    error = RuntimeError(
                        "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                        f"{advertiser_id} from "
                        f"{start_date} to "
                        f"{end_date} due to API error "
                        f"{message} with error code "
                        f"{code} then this request is eligible to retry."
                    )
                    error.retryable = True
                    raise error

        # Non-retryable API error
                error = RuntimeError(
                    "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                    f"{advertiser_id} from "
                    f"{start_date} to "
                    f"{end_date} due to API error "
                    f"{message} with error code "
                    f"{code} then this request is not eligible to retry."
                )
                error.retryable = False
                raise error

        # Retryable request timeout error
        except requests.exceptions.Timeout as e:
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                f"{advertiser_id} from "
                f"{start_date} to "
                f"{end_date} due to request timeout error then this request is eligible to retry."
            )
            error.retryable = True
            raise error from e

        # Retryable request connection error
        except requests.exceptions.ConnectionError as e:
            error = RuntimeError(
                "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                f"{advertiser_id} from "
                f"{start_date} to "
                f"{end_date} due to request connection error then this request is eligible to retry."
            )
            error.retryable = True
            raise error from e

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else None

        # Retryable HTTP error
            if status and status >= 500:
                error = RuntimeError(
                    "⚠️ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                    f"{advertiser_id} from "
                    f"{start_date} to "
                    f"{end_date} due to HTTP error "
                    f"{status} then this request is eligible to retry."
                )
                error.retryable = True
                raise error from e

        # Non-retryable HTTP error
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                f"{advertiser_id} from "
                f"{start_date} to "
                f"{end_date} due to HTTP error "
                f"{status} then this request is not eligible to retry."
            )
            error.retryable = False
            raise error from e

        # Unknown non-retryable error
        except Exception as e:
            error = RuntimeError(
                "❌ [EXTRACT] Failed to extract TikTok Ads campaign insights for advertiser_id "
                f"{advertiser_id} from "
                f"{start_date} to "
                f"{end_date} due to "
                f"{e}."
            )
            error.retryable = False
            raise error from e

        block = data.get("data") or {}
        batch = block.get("list", [])

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

    print(
        "✅ [EXTRACT] Successfully extracted "
        f"{len(df)} row(s) of TikTok Ads campaign insights for advertiser_id "
        f"{advertiser_id} from "
        f"{start_date} to "
        f"{end_date}."
    )

    return df