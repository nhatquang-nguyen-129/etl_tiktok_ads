import os
from pathlib import Path
import sys
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import secretmanager
from google.api_core.client_options import ClientOptions

from dags.dags_tiktok_ads import dags_tiktok_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

if not all([
    COMPANY,
    PROJECT,
    DEPARTMENT,
    ACCOUNT,
    MODE
]):
    raise EnvironmentError("❌ [MAIN] Failed to execute TikTok Ads main entrypoint due to missing required environment variables.")

def main():
    """
    Main TikTok Ads entrypoint
    ---------
    Workflow:
        1. Resolve execution time window from MODE
        2. Read & validate OS environment variables
        3. Load secrets from GCP Secret Manager
        4. Resolve advertiser_id and access_token
        5. Dispatch execution to DAG orchestrator
    Return:
        None
    """
    
    print(
        "🔄 [MAIN] Triggering to update TikTok Ads for "
        f"{ACCOUNT} account of "
        f"{DEPARTMENT} department in "
        f"{COMPANY} company with "
        f"{MODE} mode to Google Cloud project "
        f"{PROJECT}..."
    )

# Resolve input time range
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    today = datetime.now(ICT)
    
    if MODE == "today":
        start_date = end_date = today.strftime("%Y-%m-%d")

    elif MODE == "last3days":
        start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "last7days":
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    elif MODE == "thismonth":
        start_date = today.replace(day=1).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")

    elif MODE == "lastmonth":
        last_month_end = today.replace(day=1) - timedelta(days=1)
        start_date = last_month_end.replace(day=1).strftime("%Y-%m-%d")
        end_date = last_month_end.strftime("%Y-%m-%d")

    else:
        raise ValueError(
            "⚠️ [MAIN] Failed to trigger TikTok Ads main entrypoint due to unsupported mode "
            f"{MODE}."
        )
    
    print(
        "✅ [MAIN] Successfully resolved "
        f"{MODE} mode to date range from "
        f"{start_date} to "
        f"{end_date}."
    )

# Initialize Google Secret Manager
    try:
        print("🔍 [MAIN] Initialize Google Secret Manager client...")

        google_secret_client = secretmanager.SecretManagerServiceClient(
            client_options=ClientOptions(
                api_endpoint="secretmanager.googleapis.com"
            )
        )

        print("✅ [MAIN] Successfully initialized Google Secret Manager client.")
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to initialize Google Secret Manager client due to."
            f"{e}."
        )
        
# Resolve advertiser from Google Secret Manager
    try:
        secret_account_id = (
            f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
        )
        secret_account_name = (
            f"projects/{PROJECT}/secrets/{secret_account_id}/versions/latest"
        )
        
        print(
            "🔍 [MAIN] Retrieving TikTok Ads secret_account_id "
            f"{secret_account_name} from Google Secret Manager..."
        )

        secret_account_response = google_secret_client.access_secret_version(
            name=secret_account_name,
            timeout=10.0,
        )
        advertiser_id = secret_account_response.payload.data.decode("utf-8")
        
        print(
            "✅ [MAIN] Successfully retrieved TikTok Ads advertiser_id "
            f"{advertiser_id} from Google Secret Manager."
        )
    
    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve TikTok Ads account_id from Google Secret Manager due to "
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
            "🔍 [MAIN] Retrieving TikTok Ads access token with secret_token_name "
            f"{secret_token_name} from Google Secret Manager..."
        )

        secret_token_response = google_secret_client.access_secret_version(
            name=secret_token_name
        )
        access_token = secret_token_response.payload.data.decode("utf-8")
        
        print("✅ [MAIN] Successfully retrieved TikTok Ads access token from Google Secret Manager.")

    except Exception as e:
        raise RuntimeError(
            "❌ [MAIN] Failed to retrieve TikTok Ads access token from Google Secret Manager due to "
            f"{e}."
        )        

# Execute DAGS
    dags_tiktok_ads(
        access_token=access_token,
        advertiser_id=advertiser_id,
        start_date=start_date,
        end_date=end_date
    )

# Entrypoint
if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)