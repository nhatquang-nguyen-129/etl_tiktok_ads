import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import argparse
from datetime import datetime

from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_tiktok_ads import dags_campaign_insights

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
]):
    raise EnvironmentError("❌ [BACKFILL] Failed to execute TikTok Ads campaign insights backfill due to missing required environment variables.")

def backfill():
    """
    Backfill TikTok Ads campaign insights
    ---
    Principles:
        1. Resolve execution time window form CLI argument --start_date and --end_date
        2. Validate OS environment variables
        3. Load secrets from GCP Secret Manager
        4. Resolve advertiser_id and access_token
        5. Dispatch execution to DAG orchestrator
    ---
    Returns:
        None
    """

# CLI arguments parser for manual date range
    parser = argparse.ArgumentParser(
        description="Manual TikTok Ads ETL executor"
        )
    
    parser.add_argument(
        "--start_date",
        required=True,
        help="Start date in YYYY-MM-DD format"
    )
    
    
    parser.add_argument(
        "--end_date",
        required=True,
        help="End date in YYYY-MM-DD format"
    )
    
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError("❌ [BACKFILL] Failed to execute TikTok Ads campaign insights backfill due to start_date and end_date must be in YYYY-MM-DD format.")

    if start_date > end_date:
        raise ValueError("❌ [BACKFILL] Failed to execute TikTok Ads campaign insights backfill due to start_date must be less than or equal to end_date.")

    print(
        "🔄 [BACKFILL] Triggering to execute TikTok Ads campaign insights backfill for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company from "
        f"{start_date} to "
        f"{end_date} on Google Cloud Project "
        f"{PROJECT}..."
    )

# Initialize Google Secret Manager
    try:
        print("🔍 [BACKFILL] Initialize Google Secret Manager client...")        
        
        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        print("✅ [BACKFILL] Successfully initialized Google Secret Manager client.")
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to initialize Google Secret Manager client due to."
            f"{e}."
        )
        
# Resolve advertiser_id from Google Secret Manager
    try:
        secret_account_id = (
            f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
        )
        secret_account_name = (
            f"projects/{PROJECT}/secrets/{secret_account_id}/versions/latest"
        )
        
        print(
            "🔍 [BACKFILL] Retrieving TikTok Ads secret_account_id "
            f"{secret_account_name} from Google Secret Manager..."
        )    

        secret_account_response = google_secret_client.access_secret_version(
            name=secret_account_name,
            timeout=10.0,
        )
        advertiser_id = secret_account_response.payload.data.decode("utf-8")
        
        print(
            "✅ [BACKFILL] Successfully retrieved TikTok Ads advertiser_id "
            f"{advertiser_id} from Google Secret Manager."
        )
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to retrieve TikTok Ads advertiser_id from Google Secret Manager due to "
            f"{e}."
        )

# Resolve access_token from Google Secret Manager
    try:
        secret_token_id = (
            f"{COMPANY}_secret_all_tiktok_token_access_user"
        )
        secret_token_name = (
            f"projects/{PROJECT}/secrets/{secret_token_id}/versions/latest"
        )
        
        print(
            "🔍 [BACKFILL] Retrieving TikTok Ads access token with secret_token_name "
            f"{secret_token_name} from Google Secret Manager..."
        )

        secret_token_response = google_secret_client.access_secret_version(
            name=secret_token_name
        )
        access_token = secret_token_response.payload.data.decode("utf-8")
        
        print("✅ [BACKFILL] Successfully retrieved TikTok Ads access token from Google Secret Manager.")
    
    except Exception as e:
        raise RuntimeError(
            "❌ [BACKFILL] Failed to retrieve TikTok Ads access token from Google Secret Manager due to "
            f"{e}."
        )        
   
# Execute DAGS
    dags_campaign_insights(
        access_token=access_token,
        advertiser_id=advertiser_id,
        start_date=start_date,
        end_date=end_date
    )

# Entrypoint
if __name__ == "__main__":
    try:
        backfill()
    except Exception:
        sys.exit(1)