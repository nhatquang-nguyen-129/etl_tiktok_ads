"""
==================================================================
TIKTOK FETCHING MODULE
------------------------------------------------------------------
This module handles authenticated data retrieval from the TikTok 
Marketing API, consolidating all campaign, ad, creative, and metadata 
fetching logic into a unified, maintainable structure for ingestion.

It ensures reliable access to TikTok Ads data with controlled rate 
limits, standardized field mapping, and structured outputs for 
downstream enrichment and transformation stages.

✔️ Initializes secure TikTok SDK sessions and retrieves credentials  
✔️ Fetches campaign, ad, and creative data via authenticated API calls  
✔️ Handles pagination, rate limiting and error retries automatically  
✔️ Returns normalized and schema-ready DataFrames for processing  
✔️ Logs detailed runtime information for monitoring and debugging  

⚠️ This module focuses solely on data retrieval and extraction.  
It does not perform schema enforcement, data enrichment, or 
storage operations such as uploading to BigQuery.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilties for integration
import logging

# Add Python requests ultilities for integration
import requests

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# Add Google Secret Manager modules for integration
from google.cloud import secretmanager

# Add internal TikTok Ads modules for handling
from src.schema import enforce_table_schema

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1. FETCH TIKTOK ADS METADATA

# 1.1. Fetch campaign metadata for TikTok Ads
def fetch_campaign_metadata(fetch_campaign_ids: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")

    # 1.1.1. Start timing the TikTok Ads campaign metadata fetching
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.1.2. Validate input for the TikTok Ads campaign metadata fetching
        fetch_section_name = "[FETCH] Validate input for the TikTok Ads campaign metadata fetching"
        fetch_section_start = time.time()    
        try:
            if not fetch_campaign_ids:
                fetch_sections_status[fetch_section_name] = "failed"        
                print("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")
                logging.warning("⚠️ [FETCH] Empty TikTok Ads campaign_id_list provided then fetching is suspended.")
            else:
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully validated input for {len(fetch_campaign_ids)} campaign_id(s) of TikTok Ads campaign metadata fetching.")
                logging.info(f"✅ [FETCH] Successfully validated input for {len(fetch_campaign_ids)} campaign_id(s) of TikTok Ads campaign metadata fetching.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.3. Prepare fields for TikTok Ads campaign metadata fetching
        fetch_section_name = "[FETCH] Prepare fields for TikTok Ads campaign metadata fetching"
        fetch_section_start = time.time()        
        try:
            fetch_campaign_fields = [
                "advertiser_id",
                "campaign_id",
                "campaign_name",
                "operation_status",
                "objective_type",
                "create_time"
            ]
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"🔍 [FETCH] Preparing to fetch TikTok Ads campaign metadata with {fetch_campaign_fields} field(s)...")
            logging.info(f"🔍 [FETCH] Preparing to fetch TikTok Ads campaign metadata with {fetch_campaign_fields} field(s)...")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)         
    
    # 1.1.4 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()                
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")          
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2) 

    # 1.1.5. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            print(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            fetch_access_user = token_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.1.6. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            fetch_advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.7. Make TikTok Ads API call for advertiser endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for advertiser endpoint"
        fetch_section_start = time.time()     
        try: 
            print(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id}...")
            logging.info(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id}...")
            fetch_advertiser_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
            fetch_advertiser_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            }            
            fetch_advertiser_payload = {"advertiser_ids": [fetch_advertiser_id]}
            fetch_advertiser_response = requests.get(
                fetch_advertiser_url, 
                headers=fetch_advertiser_headers, 
                json=fetch_advertiser_payload
                )
            fetch_advertiser_name = fetch_advertiser_response.json()["data"]["list"][0]["name"]       
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved advertiser_name {fetch_advertiser_name} for TikTok Ads advertiser_id {fetch_advertiser_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved advertiser_name {fetch_advertiser_name} for TikTok Ads advertiser_id {fetch_advertiser_id}.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.8. Make TikTok Ads API call for campaign endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for campaign metadata"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads campaign metadata for {len(fetch_campaign_ids)} campaign_id(s)...")
            fetch_campaign_metadatas = []
            fetch_campaign_url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
            fetch_campaign_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            }
            for fetch_campaign_id in fetch_campaign_ids:
                try:
                    fetch_campaign_payload = {
                        "advertiser_id": fetch_advertiser_id,
                        "filtering": {"campaign_ids": [fetch_campaign_id]},
                        "fields": fetch_campaign_fields
                    }
                    fetch_campaign_response = requests.get(
                        fetch_campaign_url, 
                        headers=fetch_campaign_headers, 
                        json=fetch_campaign_payload
                        )
                    fetch_campaign_response.raise_for_status()
                    fetch_campaign_json = fetch_campaign_response.json()
                    fetch_campaign_metadata = fetch_campaign_json["data"]["list"][0]
                    fetch_campaign_metadata["advertiser_name"] = fetch_advertiser_name
                    fetch_campaign_metadatas.append(fetch_campaign_metadata)
                except Exception as e:
                    print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign metadata for campaign_id {fetch_campaign_id} due to {e}.")
                    logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads campaign metadata for campaign_id {fetch_campaign_id} due to {e}.")
            fetch_df_flattened = pd.DataFrame(fetch_campaign_metadatas)
            if len(fetch_campaign_metadatas) == len(fetch_campaign_ids):
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully retrieved TikTok Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for advertiser_id {fetch_advertiser_id}.")
                logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for advertiser_id {fetch_advertiser_id}.")
            elif len(fetch_campaign_ids) > 0 and len(fetch_campaign_metadatas) < len(fetch_campaign_ids):
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially retrieved TikTok Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for advertiser_id {fetch_advertiser_id}.")
                logging.warning(f"⚠️ [FETCH] Partially retrieved TikTok Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for advertiser_id {fetch_advertiser_id}.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for advertiser_id {fetch_advertiser_id}.")
                logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign metadata with {len(fetch_campaign_metadatas)}/{len(fetch_campaign_ids)} campaign_id(s) for advertiser_id {fetch_advertiser_id}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.9. Trigger to enforce schema for TikTok Ads campaign metadata
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads campaign metadata"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads campaign metadata with {len(fetch_df_flattened)} retrieved row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads campaign metadata with {len(fetch_df_flattened)} retrieved row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_campaign_metadata")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered TikTok Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered TikTok Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered TikTok Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered TikTok Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger TikTok Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger TikTok Ads campaign metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.1.10. Summarize fetch results for TikTok Ads campaign metadata
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_input = len(fetch_campaign_ids)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"
            print(f"❌ [FETCH] Failed to complete TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        elif fetch_rows_output == fetch_rows_input:
            fetch_status_final = "fetch_succeed_all"
            print(f"🏆 [FETCH] Successfully completed TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTOk Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")            
        else:
            fetch_status_final = "fetch_succeed_partial"
            print(f"⚠️ [FETCH] Partially completed TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads campaign metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")           
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_input": fetch_rows_input, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 1.2. Fetch ad metadata for TikTok Ads
def fetch_ad_metadata(fetch_ad_ids: list[str]) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads ad metadata for {len(fetch_ad_ids)} ad_id(s)...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads ad metadata for {len(fetch_ad_ids)} ad_id(s)...")

    # 1.2.1. Start timing the TikTok Ads ad metadata fetching
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad metadata at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.2.2. Validate input for TikTok Ads ad metadata fetching
        fetch_section_name = "[FETCH] Validate input for TikTok Ads ad metadata fetching"
        fetch_section_start = time.time()        
        try:
            if not fetch_ad_ids:
                fetch_sections_status[fetch_section_name] = "failed"        
                print("⚠️ [FETCH] Empty TikTok Ads fetch_ad_ids list provided then fetching will be suspended.")
                ("⚠️ [FETCH] Empty TikTok Ads fetch_ad_ids list provided then fetching will be suspended.")
            else:
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully validated input for {len(fetch_ad_ids)} ad_id(s) of TikTok Ads ad metadata fetching.")
                logging.info(f"✅ [FETCH] Successfully validated input for {len(fetch_ad_ids)} ad_id(s) of TikTok Ads ad metadata fetching.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.3. Prepare fields for TikTok Ads ad metadata fetching
        fetch_section_name = "[FETCH] Prepare fields for TikTok Ads ad metadata fetching"
        fetch_section_start = time.time()
        try:
            fetch_ad_fields = [
                "advertiser_id",
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
                "video_id"
            ]
            fetch_sections_status[fetch_section_name] = "succeed" 
            print(f"🔍 [FETCH] Preparing to fetch TikTok Ads ad metadata with {fetch_ad_fields} field(s)...")
            logging.info(f"🔍 [FETCH] Preparing to fetch TikTok Ads ad metadata with {fetch_ad_fields} field(s)...")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.4 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()          
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2) 

    # 1.2.5. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            print(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            fetch_access_user = token_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.6. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            fetch_advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.2.7. Make TikTok Ads API call for advertiser endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for advertiser endpoint"
        fetch_section_start = time.time()     
        try: 
            print(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id}...")
            logging.info(f"🔍 [FETCH] Retrieving advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id}...")
            fetch_advertiser_url = "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/"
            fetch_advertiser_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            }            
            fetch_advertiser_payload = {"advertiser_ids": [fetch_advertiser_id]}
            fetch_advertiser_response = requests.get(
                fetch_advertiser_url, 
                headers=fetch_advertiser_headers, 
                json=fetch_advertiser_payload
                )
            fetch_advertiser_name = fetch_advertiser_response.json()["data"]["list"][0]["name"]       
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved advertiser_name {fetch_advertiser_name} for TikTok Ads advertiser_id {fetch_advertiser_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved advertiser_name {fetch_advertiser_name} for TikTok Ads advertiser_id {fetch_advertiser_id}.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to fetch advertiser_name for TikTok Ads advertiser_id {fetch_advertiser_id} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.8. Make TikTok Ads API call for ad endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for ad endpoint"
        fetch_section_start = time.time()            
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads ad metadata for {len(fetch_ad_ids)} ad_id(s)...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads ad metadata for {len(fetch_ad_ids)} ad_id(s)...")
            fetch_ad_metadatas = []
            fetch_ad_url = "https://business-api.tiktok.com/open_api/v1.3/ad/get/"
            fetch_ad_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            }
            for fetch_ad_id in fetch_ad_ids:
                try:
                    fetch_ad_payload = {
                        "advertiser_id": fetch_advertiser_id,
                        "filtering": {"ad_ids": [fetch_ad_id]},
                        "fields": fetch_ad_fields
                    }
                    fetch_ad_response = requests.get(
                        fetch_ad_url, 
                        headers=fetch_ad_headers, 
                        json=fetch_ad_payload
                        )
                    fetch_ad_response.raise_for_status()
                    fetch_ad_json = fetch_ad_response.json()
                    fetch_ad_metadata = fetch_ad_json["data"]["list"][0]
                    fetch_ad_metadata["advertiser_name"] = fetch_advertiser_name
                    fetch_ad_metadatas.append(fetch_ad_metadata)                    
                except Exception as e:
                    print(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad metadata for ad_id {fetch_ad_id} due to {e}.")
                    logging.warning(f"⚠️ [FETCH] Failed to retrieve TikTok Ads ad metadata for ad_id {fetch_ad_id} due to {e}.")
            fetch_df_flattened = pd.DataFrame(fetch_ad_metadatas)
            if len(fetch_ad_metadatas) == len(fetch_ad_ids):
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully retrieved TikTok Ads ad metadata with {len(fetch_ad_metadatas)}/{len(fetch_ad_ids)} ad_id(s) for advertiser_id {fetch_advertiser_id}.")
                logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads ad metadata with {len(fetch_ad_metadatas)}/{len(fetch_ad_ids)} ad_id(s) for advertiser_id {fetch_advertiser_id}.")
            elif len(fetch_ad_ids) > 0 and len(fetch_ad_metadatas) < len(fetch_ad_ids):
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially retrieved TikTok Ads ad metadata with {len(fetch_ad_metadatas)}/{len(fetch_ad_ids)} ad_id(s) for advertiser_id {fetch_advertiser_id}.")
                logging.warning(f"⚠️ [FETCH] Partially retrieved TikTok Ads ad metadata with {len(fetch_ad_metadatas)}/{len(fetch_ad_ids)} ad_id(s) for advertiser_id {fetch_advertiser_id}.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad metadata with {len(fetch_ad_metadatas)}/{len(fetch_ad_ids)} ad_id(s) for advertiser_id {fetch_advertiser_id}.")
                logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad metadata with {len(fetch_ad_metadatas)}/{len(fetch_ad_ids)} ad_id(s) for advertiser_id {fetch_advertiser_id}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.9. Trigger to enforce schema for TikTok Ads ad metadata
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads ad metadata"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad metadata with {len(fetch_df_flattened)} retrieved row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad metadata with {len(fetch_df_flattened)} retrieved row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_ad_metadata")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered TikTok Ads ad metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered TikTok Ads ad metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered TikTok Ads ad metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered TikTok Ads ad metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger TikTok Ads ad metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger TikTok Ads ad metadata schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.2.10. Summarize fetch results for TikTok Ads ad metadata
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_input = len(fetch_ad_ids)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"
            print(f"❌ [FETCH] Failed to complete TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        elif fetch_rows_output == fetch_rows_input:
            fetch_status_final = "fetch_succeed_all"
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")  
        else:
            fetch_status_final = "fetch_succeed_partial"
            print(f"⚠️ [FETCH] Partially completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads ad metadata fetching with {fetch_rows_output}/{fetch_rows_input} fetched row(s) in {fetch_time_elapsed}s.")           
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_input": fetch_rows_input, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 1.3. Fetch ad creative for TikTok Ads
def fetch_ad_creative() -> pd.DataFrame:
    print("🚀 [FETCH] Starting to fetch TikTok Ads ad creative...")
    logging.info("🚀 [FETCH] Starting to fetch TikTok Ads ad creative...")

    # 1.3.1. Start timing the TikTok Ads ad creative
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad creative at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad creative at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.3.2 Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()                
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")            
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.3. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            print(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")            
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            fetch_access_user = token_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.4. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            fetch_advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.5. Make TikTok Ads API call for file/video/ad/search endpoint
        fetch_section_name = "[FETCH] Make TikTok Ads API call for file/video/ad/search endpoint"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads video creative for advertiser_id {fetch_advertiser_id}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads video creative for advertiser_id {fetch_advertiser_id}...")
            fetch_ad_creatives = []
            fetch_video_url = "https://business-api.tiktok.com/open_api/v1.3/file/video/ad/search/"
            fetch_video_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            }
            fetch_pagination_current = 1
            fetch_pagination_continue = True
            while fetch_pagination_continue:
                fetch_video_payload = {
                    "advertiser_id": fetch_advertiser_id,
                    "page_size": 100,
                    "page": fetch_pagination_current
                }
                fetch_video_response = requests.get(
                    fetch_video_url, 
                    headers=fetch_video_headers, 
                    json=fetch_video_payload
                    )
                fetch_video_response.raise_for_status()
                fetch_video_json = fetch_video_response.json()
                if fetch_video_json.get("code") == 0 and fetch_video_json.get("data", {}).get("list"):
                    for fetch_video_record in fetch_video_json["data"]["list"]:
                        fetch_ad_creatives.append({
                            "advertiser_id": fetch_advertiser_id,
                            "video_id": fetch_video_record.get("video_id"),
                            "video_cover_url": fetch_video_record.get("video_cover_url"),
                            "preview_url": fetch_video_record.get("preview_url"),
                            "create_time": fetch_video_record.get("create_time")
                        })
                    page_info = fetch_video_json["data"].get("page_info", {})
                    total_page = page_info.get("total_page", 1)
                    fetch_pagination_continue = fetch_pagination_current < total_page
                    fetch_pagination_current += 1
                else:
                    fetch_pagination_continue = False
            fetch_df_flattened = pd.DataFrame(fetch_ad_creatives)
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads ad creative for {len(fetch_df_flattened)} row(s) for TikTok Ads advertiser_id {fetch_advertiser_id}.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads ad creative for {len(fetch_df_flattened)} row(s) for TikTok Ads advertiser_id {fetch_advertiser_id}.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad creative for advertiser_id {fetch_advertiser_id} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad creative for advertiser_id {fetch_advertiser_id} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)
    
    # 1.3.6. Trigger to enforce schema for TikTok Ads ad creative
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads ad creative"
        fetch_section_start = time.time()
        try:
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad creative with {len(fetch_df_flattened)} retrieved row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad creative with {len(fetch_df_flattened)} retrieved row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_ad_creative")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered TikTok Ads ad creative schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered TikTok Ads ad creative schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered TikTok Ads ad creative schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered TikTok Ads ad creative schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger TikTok Ads ad creative schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger TikTok Ads ad creative schema enforcement with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 1.3.7. Summarize fetch results for TikTok Ads ad creative
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }          
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"
            print(f"❌ [FETCH] Failed to complete TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        else:
            fetch_status_final = "fetch_succeed_all"
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads ad creative fetching with {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed, 
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded, 
                "fetch_sections_failed": fetch_sections_failed, 
                "fetch_sections_detail": fetch_sections_detail, 
                "fetch_rows_output": fetch_rows_output
            },
        }
    return fetch_results_final

# 2. FETCH TIKTOK ADS INSIGHTS

# 2.1. Fetch campaign insights for TikTok Ads
def fetch_campaign_insights(fetch_date_start: str, fetch_date_end: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")      

    # 2.1.1. Start timing the TikTok Ads campaign insights fetching
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:
        
    # 2.1.2. Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")            
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.3. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            print(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            fetch_access_user = token_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.4. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            fetch_advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.5. Make TikTok Ads API call for campaign insights
        fetch_section_name = "[FETCH] Make TikTok Ads API call for campaign insights"
        fetch_section_start = time.time()
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads campaign insights for advertiser_id {fetch_advertiser_id} from {fetch_date_start} to {fetch_date_end}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads campaign insights for advertiser_id {fetch_advertiser_id} from {fetch_date_start} to {fetch_date_end}...")
            fetch_attempts_queued = 2
            fetch_campaign_insights = []
            fetch_campaign_records = []         
            fetch_campaign_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
            fetch_campaign_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            } 
            fetch_campaign_params = {
                "advertiser_id": fetch_advertiser_id,
                "report_type": "BASIC",
                "data_level": "AUCTION_CAMPAIGN",
                "dimensions": ["campaign_id", "stat_time_day"],
                "metrics": [
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
                    "messaging_total_conversation_tiktok_direct_message"
                ],
                "start_date": fetch_date_start,
                "end_date": fetch_date_end,
                "page_size": 1000,
                "page": 1
            }  
            for fetch_attempt_queued in range(fetch_attempts_queued):
                try:
                    while True:
                        fetch_campaign_response = requests.get(
                            fetch_campaign_url,
                            headers=fetch_campaign_headers,
                            json=fetch_campaign_params,
                            timeout=60
                        )
                        fetch_campaign_json = fetch_campaign_response.json()
                        if fetch_campaign_json.get("code") != 0:
                            raise Exception(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign insights due to API error {fetch_campaign_json.get('message')}.")
                        fetch_campaign_batch = fetch_campaign_json["data"].get("list", [])
                        fetch_campaign_records.extend(fetch_campaign_batch)
                        if len(fetch_campaign_batch) < fetch_campaign_params["page_size"]:
                            break
                        fetch_campaign_params["page"] += 1                  
                    for fetch_record_campaign in fetch_campaign_records:
                        fetch_campaign_insight = {}
                        fetch_campaign_insight.update(fetch_record_campaign.get("dimensions", {}))
                        fetch_campaign_insight.update(fetch_record_campaign.get("metrics", {}))
                        fetch_campaign_insight["advertiser_id"] = fetch_campaign_params["advertiser_id"]
                        fetch_campaign_insights.append(fetch_campaign_insight)
                    fetch_df_flattened = pd.DataFrame(fetch_campaign_insights)
                    fetch_sections_status[fetch_section_name] = "succeed"
                    print(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads campaign insights.")
                    logging.info(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads campaign insights.")
                    break
                except Exception as e:
                    if fetch_attempt_queued < fetch_attempts_queued - 1:
                        fetch_attempt_delayed = 60 + (fetch_attempt_queued * 60)
                        print(f"🔄 [FETCH] Waiting {fetch_attempt_delayed}s before retrying to retrieve TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")
                        logging.warning(f"🔄 [FETCH] Waiting {fetch_attempt_delayed}s before retrying to retrieve TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end}...")
                        time.sleep(fetch_attempt_delayed)                    
                    else:
                        fetch_sections_status[fetch_section_name] = "failed"
                        print(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end} due to maximum retry attempts exceeded.")
                        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads campaign insights from {fetch_date_start} to {fetch_date_end} due to maximum retry attempts exceeded.")
        finally:
            fetch_cooldown_queued = 60 + 30 * max(0, fetch_attempt_queued)
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)                     
   
    # 2.1.6. Trigger to enforce schema for TikTok Ads campaign insights
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads campaign insights"
        fetch_section_start = time.time()        
        try:            
            print(f"🔄 [FETCH] Trigger to enforce schema for TiKTok Ads campaign insights from {fetch_date_start} to {fetch_date_end} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TiKTok Ads campaign insights from {fetch_date_start} to {fetch_date_end} with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_campaign_insights")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered TikTok Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered TikTok Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered TikTok Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered TikTok Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger TikTok Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger TikTok Ads campaign insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.1.7. Summarize fetch results for TikTok Ads campaign insights
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_days_input = ((pd.to_datetime(fetch_date_end) - pd.to_datetime(fetch_date_start)).days + 1)
        fetch_days_output = (fetch_df_final["stat_time_day"].nunique() if not fetch_df_final.empty and "stat_time_day" in fetch_df_final.columns else 0)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }        
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"            
            print(f"❌ [FETCH] Failed to complete TikTok Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        elif fetch_days_output == fetch_days_input:
            fetch_status_final = "fetch_succeed_all" 
            print(f"🏆 [FETCH] Successfully completed TikTok Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
        else:
            fetch_status_final = "fetch_succeed_partial"
            print(f"⚠️ [FETCH] Partially completed TikTok Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads campaign insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")            
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed,
                "fetch_cooldown_queued": fetch_cooldown_queued,                
                "fetch_days_input": fetch_days_input,
                "fetch_days_output": fetch_days_output,
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded,
                "fetch_sections_failed": fetch_sections_failed,
                "fetch_sections_detail": fetch_sections_detail,
                "fetch_rows_output": fetch_rows_output,
            },
        }
    return fetch_results_final

# 2.2. Fetch ad insights for TikTok Ads    
def fetch_ad_insights(fetch_date_start: str, fetch_date_end: str) -> pd.DataFrame:
    print(f"🚀 [FETCH] Starting to fetch TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end}...")
    logging.info(f"🚀 [FETCH] Starting to fetch TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end}...")       

    # 2.2.1. Start timing the TikTok Ads ad insights fetching
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    fetch_time_start = time.time()   
    fetch_sections_status = {}
    fetch_sections_time = {}
    print(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [FETCH] Proceeding to fetch TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 2.2.2. Initialize Google Secret Manager client
        fetch_section_name = "[FETCH] Initialize Google Secret Manager client"
        fetch_section_start = time.time()           
        try:
            print(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [FETCH] Initializing Google Secret Manager client for Google Cloud Platform project {PROJECT}...")
            google_secret_client = secretmanager.SecretManagerServiceClient()
            fetch_sections_status[fetch_section_name] = "succeed"
            print(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
            logging.info(f"✅ [FETCH] Successfully initialized Google Secret Manager client for Google Cloud project {PROJECT}.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [FETCH] Failed to initialize Google Secret Manager client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.3. Get TikTok Ads access token from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads access token from Google Secret Manager"
        fetch_section_start = time.time()              
        try: 
            print(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads access token for account {ACCOUNT}...")
            token_secret_id = f"{COMPANY}_secret_all_{PLATFORM}_token_access_user"
            token_secret_name = f"projects/{PROJECT}/secrets/{token_secret_id}/versions/latest"
            token_secret_response = google_secret_client.access_secret_version(request={"name": token_secret_name})
            fetch_access_user = token_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads access token for account {ACCOUNT} from Google Secret Manager.")
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok access token for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.4. Get TikTok Ads advertiser_id from Google Secret Manager
        fetch_section_name = "[FETCH] Get TikTok Ads advertiser_id from Google Secret Manager"
        fetch_section_start = time.time()        
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads advertiser_id for account {ACCOUNT} from Google Secret Manager...")
            advertiser_secret_id = f"{COMPANY}_secret_{DEPARTMENT}_tiktok_account_id_{ACCOUNT}"
            advertiser_secret_name = f"projects/{PROJECT}/secrets/{advertiser_secret_id}/versions/latest"
            advertiser_secret_response = google_secret_client.access_secret_version(request={"name": advertiser_secret_name})
            fetch_advertiser_id = advertiser_secret_response.payload.data.decode("utf-8")
            fetch_sections_status[fetch_section_name] = "succeed"            
            print(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")
            logging.info(f"✅ [FETCH] Successfully retrieved TikTok Ads advertiser_id {fetch_advertiser_id} from Google Secret Manager.")           
        except Exception as e:
            fetch_sections_status[fetch_section_name] = "failed"
            print(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
            logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads advertiser_id for {ACCOUNT} from Google Secret Manager due to {e}.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.5. Make TikTok Ads API call for ad insights
        fetch_section_name = "[FETCH] Make TikTok Ads API call for ad insights"
        fetch_section_start = time.time()
        try:
            print(f"🔍 [FETCH] Retrieving TikTok Ads ad insights for advertiser_id {fetch_advertiser_id} from {fetch_date_start} to {fetch_date_end}..")
            logging.info(f"🔍 [FETCH] Retrieving TikTok Ads ad insights for advertiser_id {fetch_advertiser_id} from {fetch_date_start} to {fetch_date_end}..")
            fetch_attempts_queued = 2
            fetch_ad_insights = []
            fetch_ad_records = []     
            fetch_ad_url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
            fetch_ad_headers = {
                "Access-Token": fetch_access_user,
                "Content-Type": "application/json"
            }             
            fetch_ad_params = {
                "advertiser_id": fetch_advertiser_id,
                "report_type": "BASIC",
                "data_level": "AUCTION_AD",
                "dimensions": ["ad_id", "stat_time_day"],
                "metrics": [
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
                    "messaging_total_conversation_tiktok_direct_message"
                ],
                "start_date": fetch_date_start,
                "end_date": fetch_date_end,
                "page_size": 1000,
                "page": 1
            }       
            for fetch_attempt_queued in range(fetch_attempts_queued):
                try:
                    while True:
                        fetch_ad_response = requests.get(
                            fetch_ad_url,
                            headers=fetch_ad_headers,
                            json=fetch_ad_params,
                            timeout=60
                        )
                        fetch_ad_json = fetch_ad_response.json()                  
                        if fetch_ad_json.get("code") != 0:
                            raise Exception(f"❌ [FETCH] Failed to retrieve TikTok Ads ad-level insights with BASIC report_type due to API error {fetch_ad_json.get('message')}.")
                        fetch_ad_batch = fetch_ad_json["data"].get("list", [])
                        fetch_ad_records.extend(fetch_ad_batch)
                        if len(fetch_ad_batch) < fetch_ad_params["page_size"]:
                            break
                        fetch_ad_params["page"] += 1
                    for fetch_ad_record in fetch_ad_records:
                        fetch_ad_insight = {}
                        fetch_ad_insight.update(fetch_ad_record.get("dimensions", {}))
                        fetch_ad_insight.update(fetch_ad_record.get("metrics", {}))
                        fetch_ad_insight["advertiser_id"] = fetch_ad_params["advertiser_id"]
                        fetch_ad_insights.append(fetch_ad_insight)
                    fetch_df_flattened = pd.DataFrame(fetch_ad_insights)
                    fetch_sections_status[fetch_section_name] = "succeed"
                    print(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads ad insights.")
                    logging.info(f"✅ [FETCH] Successfully retrieved {len(fetch_df_flattened)} rows of TikTok Ads ad insights.")                    
                    break
                except Exception as e:
                    if fetch_attempt_queued < fetch_attempts_queued - 1:
                        fetch_attempt_delayed = 60 + (fetch_attempt_queued * 60)
                        print(f"🔄 [FETCH] Waiting {fetch_attempt_delayed}s before retrying to retrieve TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end}...")
                        logging.warning(f"🔄 [FETCH] Waiting {fetch_attempt_delayed}s before retrying to retrieve TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end}...")
                        time.sleep(fetch_attempt_delayed)                    
                    else:
                        fetch_sections_status[fetch_section_name] = "failed"
                        print(f"❌ [FETCH] Failed to retrieve TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end} due to maximum retry attempts exceeded.")
                        logging.error(f"❌ [FETCH] Failed to retrieve TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end} due to maximum retry attempts exceeded.")
        finally:
            fetch_cooldown_queued = 60 + 30 * max(0, fetch_attempt_queued)
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)              
    
    # 2.2.6. Trigger to enforce schema for TikTok Ads ad insights
        fetch_section_name = "[FETCH] Trigger to enforce schema for TikTok Ads ad insights"
        fetch_section_start = time.time()        
        try:            
            print(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end} with {len(fetch_df_flattened)} row(s)...")
            logging.info(f"🔄 [FETCH] Trigger to enforce schema for TikTok Ads ad insights from {fetch_date_start} to {fetch_date_end} with {len(fetch_df_flattened)} row(s)...")
            fetch_results_schema = enforce_table_schema(fetch_df_flattened, "fetch_ad_insights")            
            fetch_summary_enforced = fetch_results_schema["schema_summary_final"]
            fetch_status_enforced = fetch_results_schema["schema_status_final"]
            fetch_df_enforced = fetch_results_schema["schema_df_final"]    
            if fetch_status_enforced == "schema_succeed_all":
                fetch_sections_status[fetch_section_name] = "succeed"
                print(f"✅ [FETCH] Successfully triggered TikTok Ads ad insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.info(f"✅ [FETCH] Successfully triggered TikTok Ads ad insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            elif fetch_status_enforced == "schema_succeed_partial":
                fetch_sections_status[fetch_section_name] = "partial"
                print(f"⚠️ [FETCH] Partially triggered TikTok Ads ad insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.warning(f"⚠️ [FETCH] Partially triggered TikTok Ads ad insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
            else:
                fetch_sections_status[fetch_section_name] = "failed"
                print(f"❌ [FETCH] Failed to trigger TikTok Ads ad insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
                logging.error(f"❌ [FETCH] Failed to trigger TikTok Ads ad insights schema enforcement from {fetch_date_start} to {fetch_date_end} with {fetch_summary_enforced['schema_rows_output']}/{fetch_summary_enforced['schema_rows_input']} enforced row(s) in {fetch_summary_enforced['schema_time_elapsed']}s.")
        finally:
            fetch_sections_time[fetch_section_name] = round(time.time() - fetch_section_start, 2)

    # 2.2.7. Summarize fetch results for TikTok Ads ad insights
    finally:
        fetch_time_elapsed = round(time.time() - fetch_time_start, 2)
        fetch_df_final = fetch_df_enforced.copy() if "fetch_df_enforced" in locals() and not fetch_df_enforced.empty else pd.DataFrame()
        fetch_sections_total = len(fetch_sections_status) 
        fetch_sections_failed = [k for k, v in fetch_sections_status.items() if v == "failed"] 
        fetch_sections_succeeded = [k for k, v in fetch_sections_status.items() if v == "succeed"]
        fetch_days_input = ((pd.to_datetime(fetch_date_end) - pd.to_datetime(fetch_date_start)).days + 1)
        fetch_days_output = (fetch_df_final["stat_time_day"].nunique() if not fetch_df_final.empty and "stat_time_day" in fetch_df_final.columns else 0)
        fetch_rows_output = len(fetch_df_final)
        fetch_sections_summary = list(dict.fromkeys(
            list(fetch_sections_status.keys()) +
            list(fetch_sections_time.keys())
        ))
        fetch_sections_detail = {
            fetch_section_summary: {
                "status": fetch_sections_status.get(fetch_section_summary, "unknown"),
                "time": fetch_sections_time.get(fetch_section_summary, None),
            }
            for fetch_section_summary in fetch_sections_summary
        }        
        if fetch_sections_failed:
            fetch_status_final = "fetch_failed_all"
            print(f"❌ [FETCH] Failed to complete TikTok Ads ad insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
            logging.error(f"❌ [FETCH] Failed to complete TikTok Ads ad insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) due to {', '.join(fetch_sections_failed)} failed section(s) in {fetch_time_elapsed}s.")
        elif fetch_days_output == fetch_days_input:
            fetch_status_final = "fetch_succeed_all"
            print(f"🏆 [FETCH] Successfully completed TikTok Ads ad insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.info(f"🏆 [FETCH] Successfully completed TikTok Ads ad insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
        else:
            fetch_status_final = "fetch_succeed_partial"
            print(f"⚠️ [FETCH] Partially completed TikTok Ads ad insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
            logging.warning(f"⚠️ [FETCH] Partially completed TikTok Ads ad insights fetching from {fetch_date_start} to {fetch_date_end} with {fetch_days_output}/{fetch_days_input} fetched day(s) and {fetch_rows_output} fetched row(s) in {fetch_time_elapsed}s.")
        fetch_results_final = {
            "fetch_df_final": fetch_df_final,
            "fetch_status_final": fetch_status_final,
            "fetch_summary_final": {
                "fetch_time_elapsed": fetch_time_elapsed,
                "fetch_cooldown_queued": fetch_cooldown_queued,                
                "fetch_days_input": fetch_days_input,
                "fetch_days_output": fetch_days_output,
                "fetch_sections_total": fetch_sections_total,
                "fetch_sections_succeed": fetch_sections_succeeded,
                "fetch_sections_failed": fetch_sections_failed,
                "fetch_sections_detail": fetch_sections_detail,
                "fetch_rows_output": fetch_rows_output,
            },
        }
    return fetch_results_final