"""
==================================================================
TIKTOK MATERIALIZATION MODULE
------------------------------------------------------------------
This module materializes the final layer for TikTok Ads by 
aggregating and transforming data sourced from staging tables 
produced during the raw data ingestion process.

It serves as the final transformation stage, consolidating daily 
performance and cost metrics into analytics-ready BigQuery tables 
optimized for reporting, dashboarding, and business analysis.

✔️ Dynamically identifies all available TikTok Ads staging tables  
✔️ Applies data transformation, standardization, and type enforcement  
✔️ Performs daily-level aggregation of campaign performance metrics  
✔️ Creates partitioned and clustered materialized tables
✔️ Ensures consistency and traceability across the data pipeline  

⚠️ This module is exclusively responsible for materialized layer  
construction. It does not perform data ingestion, API fetching, 
or enrichment tasks.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities for integraton
import logging

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Google Cloud modules for integration
from google.cloud import bigquery

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

# 1. BUILD MONTHLY MATERIALIZED TABLE FOR TIKTOK ADS CAMPAIGN PERFORMANCE

# 1.1. Build materialzed table for TikTok Ads campaign performance by union all staging tables
def mart_campaign_all() -> dict:
    print(f"🚀 [MART] Starting to build materialized table for TikTok Ads campaign performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table TikTok Ads campaign performance...")

    # 1.1.1. Start timing the TikTok Ads campaign materialization
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for TikTok Ads campaign performance at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for TikTok Ads campaign performance at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.1.2. Prepare Google BigQuery table_id for materialization
        mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
        mart_section_start = time.time()
        try: 
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_campaign = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_insights"
            print(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for TikTok Ads campaign performance...")
            logging.info(f"🔍 [MART] Using staging table {staging_table_campaign} to build materialized table for TikTok Ads campaign performance...")
            mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
            mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_campaign_performance"
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for TikTok Ads campaign performance...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for TikTok Ads campaign performance...")
            mart_sections_status[mart_section_name] = "succeed"    
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)
    
    # 1.1.4. Query all staging TikTok Ads campaign insights tables
        mart_section_name = "[MART] Query all staging TikTok Ads campaign insights tables"
        mart_section_start = time.time()    
        try:
            query_replace_config = f"""
                CREATE OR REPLACE TABLE `{mart_table_all}`
                PARTITION BY ngay
                CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, hang_muc
                AS
                WITH base AS (
                    SELECT
                        SAFE_CAST(enrich_account_name AS STRING) AS tai_khoan,
                        SAFE_CAST(enrich_account_department AS STRING) AS phong_ban,
                        SAFE_CAST(enrich_account_platform AS STRING) AS nen_tang,
                        SAFE_CAST(enrich_budget_group AS STRING) AS ma_ngan_sach_cap_1,
                        SAFE_CAST(enrich_budget_type AS STRING) AS ma_ngan_sach_cap_2,
                        SAFE_CAST(enrich_program_track AS STRING) AS hang_muc,
                        SAFE_CAST(enrich_program_group AS STRING) AS chuong_trinh,
                        SAFE_CAST(enrich_program_type AS STRING) AS noi_dung,
                        SAFE_CAST(enrich_campaign_objective AS STRING) AS hinh_thuc,
                        SAFE_CAST(enrich_campaign_region AS STRING) AS khu_vuc,
                        SAFE_CAST(enrich_campaign_personnel AS STRING) AS nhan_su,
                        SAFE_CAST(enrich_category_group AS STRING) AS nganh_hang,
                        SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                        CAST(date AS DATE) AS ngay,
                        SAFE_CAST(year AS STRING) AS nam,
                        SAFE_CAST(month AS STRING) AS thang,
                        SAFE_CAST(spend AS FLOAT64) AS spend,
                        SAFE_CAST(result AS INT64) AS result,
                        SAFE_CAST(objective_type AS STRING) AS result_type,
                        SAFE_CAST(impressions AS INT64) AS impressions,
                        SAFE_CAST(clicks AS INT64) AS clicks,
                        SAFE_CAST(engaged_view_15s AS INT64) AS engaged_view_15s,
                        SAFE_CAST(purchase AS INT64) AS purchase,
                        CASE
                            WHEN REGEXP_CONTAINS(delivery_status, r"ENABLE") THEN "🟢"
                            WHEN REGEXP_CONTAINS(delivery_status, r"DISABLE") THEN "⚪"
                            ELSE "❓"
                        END AS trang_thai
                    FROM `{staging_table_campaign}`
                    WHERE date IS NOT NULL
                )
                SELECT *
                FROM base;
            """
            print(f"🔄 [MART] Querying staging TikTok Ads campaign insights table {staging_table_campaign} to create or replace materialized table for campaign performance...")
            logging.info(f"🔄 [MART] Querying staging TikTok Ads campaign insights table {staging_table_campaign} to create or replace materialized table for campaign performance...")
            query_replace_load = google_bigquery_client.query(query_replace_config)
            query_replace_result = query_replace_load.result()
            query_count_config = f"SELECT COUNT(1) AS mart_rows_count FROM `{mart_table_all}`"
            query_count_load = google_bigquery_client.query(query_count_config)
            query_count_result = query_count_load.result()
            mart_rows_uploaded = list(query_count_result)[0]["mart_rows_count"]
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for TikTok Ads campaign performance with {mart_rows_uploaded} row(s).")
            logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for TikTok Ads campaign performance with {mart_rows_uploaded} row(s).")            
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for TikTok Ads campaign performance due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for TikTok Ads campaign performance due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 1.1.5. Summarize materialization results for TikTok Ads campaign performance
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_rows_output = mart_rows_uploaded
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }       
        if mart_sections_failed:
            mart_status_final = "mart_failed_all"
            print(f"❌ [MART] Failed to complete TikTok Ads campaign performance materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
            logging.error(f"❌ [MART] Failed to complete TikTok Ads campaign performance materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
        else:
            mart_status_final = "mart_succeed_all"
            print(f"🏆 [MART] Successfully completed TikTok Ads campaign performance materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed TikTok Ads campaign performance materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
                "mart_rows_output": mart_rows_output,
            },
        }
    return mart_results_final

# 2. BUILD MATERIALIZED TABLE FOR TIKTOK ADS CREATIVE PERFORMANCE

# 2.1. Build materialized table for TikTok creative performance by union all staging tables
def mart_creative_all() -> dict:
    print(f"🚀 [MART] Starting to build materialized table for TikTok Ads creative performance...")
    logging.info(f"🚀 [MART] Starting to build materialized table for TikTok Ads creative performance...")

    # 2.1.1. Start timing the TikTok Ads creative performance materialization
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    mart_time_start = time.time()
    mart_sections_status = {}
    mart_sections_time = {}
    print(f"🔍 [MART] Proceeding to build materialized table for TikTok Ads creative performance at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [MART] Proceeding to build materialized table for TikTok Ads creative performance at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 2.1.2. Prepare Google BigQuery table_id for materialization
        mart_section_name = "[MART] Prepare Google BigQuery table_id for materialization"
        mart_section_start = time.time()        
        try:
            staging_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_staging"
            staging_table_ad = f"{PROJECT}.{staging_dataset}.{COMPANY}_table_{PLATFORM}_all_all_ad_insights"
            print(f"🔍 [MART] Using staging table {staging_table_ad} to build materialized table for TikTok Ads creative performance...")
            logging.info(f"🔍 [MART] Using staging table {staging_table_ad} to build materialized table for TikTok Ads creative performance...")
            mart_dataset = f"{COMPANY}_dataset_{PLATFORM}_api_mart"
            mart_table_all = f"{PROJECT}.{mart_dataset}.{COMPANY}_table_{PLATFORM}_all_all_creative_performance"
            print(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for TikTok Ads creative performance...")
            logging.info(f"🔍 [MART] Preparing to build materialized table {mart_table_all} for TikTok Ads creative performance...") 
            mart_sections_status[mart_section_name] = "succeed"    
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.3. Initialize Google BigQuery client
        mart_section_name = "[MART] Initialize Google BigQuery client"
        mart_section_start = time.time()    
        try:
            print(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            logging.info(f"🔍 [MART] Initializing Google BigQuery client for Google Cloud Platform project {PROJECT}...")
            google_bigquery_client = bigquery.Client(project=PROJECT)
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")
            logging.info(f"✅ [MART] Successfully initialized Google BigQuery client for Google Cloud Platform project {PROJECT}.")            
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
            logging.error(f"❌ [MART] Failed to initialize Google BigQuery client for Google Cloud Platform project {PROJECT} due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.4. Query all staging tables for materialization
        mart_section_name = "[MART] Query all staging tables for materialization"
        mart_section_start = time.time()  
        try:
            query_replace_config = f"""
                CREATE OR REPLACE TABLE `{mart_table_all}`
                PARTITION BY ngay
                CLUSTER BY nhan_su, ma_ngan_sach_cap_1, nganh_hang, hang_muc
                AS
                SELECT
                    SAFE_CAST(enrich_account_name AS STRING) AS tai_khoan,
                    SAFE_CAST(enrich_account_department AS STRING) AS phong_ban,
                    SAFE_CAST(enrich_account_platform AS STRING) AS nen_tang,
                    SAFE_CAST(enrich_budget_group AS STRING) AS ma_ngan_sach_cap_1,
                    SAFE_CAST(enrich_budget_type AS STRING) AS ma_ngan_sach_cap_2,
                    SAFE_CAST(enrich_program_track AS STRING) AS hang_muc,
                    SAFE_CAST(enrich_program_group AS STRING) AS chuong_trinh,
                    SAFE_CAST(enrich_program_type AS STRING) AS noi_dung,
                    SAFE_CAST(enrich_campaign_objective AS STRING) AS hinh_thuc,
                    SAFE_CAST(enrich_campaign_region AS STRING) AS khu_vuc,
                    SAFE_CAST(enrich_campaign_personnel AS STRING) AS nhan_su,
                    SAFE_CAST(enrich_category_group AS STRING) AS nganh_hang,
                    SAFE_CAST(enrich_adset_location AS STRING) AS vi_tri,
                    SAFE_CAST(enrich_adset_audience AS STRING) AS doi_tuong,
                    SAFE_CAST(enrich_adset_format AS STRING) AS dinh_dang,
                    SAFE_CAST(enrich_adset_strategy AS STRING) AS hoat_dong,
                    SAFE_CAST(campaign_name AS STRING) AS campaign_name,
                    SAFE_CAST(adset_name AS STRING) AS adset_name,
                    SAFE_CAST(ad_name AS STRING) AS ad_name,
                    SAFE_CAST(video_cover_url AS STRING) AS video_cover_url,
                    SAFE_CAST(optimization_event AS STRING) AS optimization_event,
                    CAST(date AS DATE) AS ngay,
                    SAFE_CAST(year AS STRING) AS nam,
                    SAFE_CAST(month AS STRING) AS thang,
                    SAFE_CAST(spend AS FLOAT64) AS spend,
                    SAFE_CAST(result AS INT64) AS result,
                    SAFE_CAST(impressions AS INT64) AS impressions,
                    SAFE_CAST(clicks AS INT64) AS clicks,
                    SAFE_CAST(engaged_view_15s AS INT64) AS engaged_view_15s,
                    SAFE_CAST(purchase AS INT64) AS purchase,
                    CASE
                        WHEN REGEXP_CONTAINS(delivery_status, r"ENABLE") THEN "🟢"
                        WHEN REGEXP_CONTAINS(delivery_status, r"DISABLE") THEN "⚪"
                        ELSE "❓"
                    END AS trang_thai
                FROM `{staging_table_ad}`
            """
            print(f"🔄 [MART] Querying staging TikTok Ads ad insights table {staging_table_ad} to create or replace materialized table for creative performance...")
            logging.info(f"🔄 [MART] Querying staging TikTok Ads ad insights table {staging_table_ad} to create or replace materialized table for creative performance...")
            query_replace_load = google_bigquery_client.query(query_replace_config)
            query_replace_result = query_replace_load.result()
            query_count_config = f"SELECT COUNT(1) AS mart_rows_count FROM `{mart_table_all}`"
            query_count_load = google_bigquery_client.query(query_count_config)
            query_count_result = query_count_load.result()
            mart_rows_uploaded = list(query_count_result)[0]["mart_rows_count"]
            mart_sections_status[mart_section_name] = "succeed"
            print(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for TikTok Ads creative performance with {mart_rows_uploaded} row(s).")
            logging.info(f"✅ [MART] Successfully created or replace materialized table {mart_table_all} for TikTok Ads creative performance with {mart_rows_uploaded} row(s).")
        except Exception as e:
            mart_sections_status[mart_section_name] = "failed"
            print(f"❌ [MART] Failed to create or replace materialized table for TikTok Ads campaign performance due to {e}.")
            logging.error(f"❌ [MART] Failed to create or replace materialized table for TikTok Ads campaign performance due to {e}.")
        finally:
            mart_sections_time[mart_section_name] = round(time.time() - mart_section_start, 2)

    # 2.1.5. Summarize materialization results for TikTok Ads creative performance
    finally:
        mart_time_elapsed = round(time.time() - mart_time_start, 2)
        mart_sections_total = len(mart_sections_status) 
        mart_sections_failed = [k for k, v in mart_sections_status.items() if v == "failed"] 
        mart_sections_succeeded = [k for k, v in mart_sections_status.items() if v == "succeed"]
        mart_rows_output = mart_rows_uploaded        
        mart_sections_summary = list(dict.fromkeys(
            list(mart_sections_status.keys()) +
            list(mart_sections_time.keys())
        ))
        mart_sections_detail = {
            mart_section_summary: {
                "status": mart_sections_status.get(mart_section_summary, "unknown"),
                "time": round(mart_sections_time.get(mart_section_summary, 0.0), 2),
            }
            for mart_section_summary in mart_sections_summary
        }          
        if mart_sections_failed:
            mart_status_final = "mart_failed_all"
            print(f"❌ [MART] Failed to complete TikTok Ads creative performance materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
            logging.error(f"❌ [MART] Failed to complete TikTok Ads creative performance materialization with {mart_rows_output} materialized row(s) due to {', '.join(mart_sections_failed)} failed section(s) in {mart_time_elapsed}s.")
        else:
            mart_status_final = "mart_succeed_all"
            print(f"🏆 [MART] Successfully completed TikTok Ads creative performance materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")
            logging.info(f"🏆 [MART] Successfully completed TikTok Ads creative performance materialization with {mart_rows_output} materialized row(s) in {mart_time_elapsed}s.")            
        mart_results_final = {
            "mart_df_final": None,
            "mart_status_final": mart_status_final,
            "mart_summary_final": {
                "mart_time_elapsed": mart_time_elapsed,
                "mart_sections_total": mart_sections_total,
                "mart_sections_succeed": mart_sections_succeeded,
                "mart_sections_failed": mart_sections_failed,
                "mart_sections_detail": mart_sections_detail,
                "mart_rows_output": mart_rows_output,
            },
        }
    return mart_results_final