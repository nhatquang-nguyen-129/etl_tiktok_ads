"""
==================================================================
TIKTOK UPDATE MODULE
------------------------------------------------------------------
This module performs incremental updates to TikTok Ads data at  
the raw layer, providing an efficient mechanism for refreshing  
recent or specific-date datasets without the need for full reloads.

By supporting targeted updates (per day, layer, or entity), it  
enables faster turnaround for near-real-time dashboards and daily  
data sync jobs while maintaining historical accuracy and integrity.

✔️ Handles incremental data ingestion from the TikTok Marketing API  
✔️ Supports selective updates for campaign, adset, ad or creative  
✔️ Preserves schema alignment with staging and MART layers  
✔️ Implements error handling and retry logic for partial failures  
✔️ Designed for integration in daily or on-demand sync pipelines  

⚠️ This module is strictly responsible for *RAW layer updates only*.  
It does not perform transformations, enrichment, or aggregations.  
Processed data is consumed by the STAGING and MART modules.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add logging ultilities for integration
import logging

# Add Python 'time' libraries for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add internal TikTok Ads module for handling
from src.ingest import (
    ingest_campaign_metadata,
    ingest_ad_metadata,
    ingest_ad_creative,
    ingest_campaign_insights,
    ingest_ad_insights,
)
from src.staging import (
    staging_campaign_insights,
    staging_ad_insights
)
from src.mart import (
    mart_campaign_all,
    mart_creative_all
)

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

# 1. UPDATE TIKTOK ADS INSIGHTS FOR A GIVEN DATE RANGE

# 1.1. Update TikTok Ads campaign insights
def update_campaign_insights(update_date_start: str, update_date_end: str):
    print(f"🚀 [UPDATE] Starting to update TikTok Ads campaign insights from {update_date_start} to {update_date_end}...")
    logging.info(f"🚀 [UPDATE] Starting to update TikTok Ads campaign insights from {update_date_start} to {update_date_end}...")

    # 1.1.1. Start timing TikTok Ads campaign insights update
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update TikTok Ads campaign insights from {update_date_start} to {update_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update TikTok Ads campaign insights from {update_date_start} to {update_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.1.2. Trigger to ingest TikTok Ads campaign insights
        update_section_name = "[UPDATE] Trigger to ingest TikTok Ads campaign insights"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end}...")
            ingest_results_insights = ingest_campaign_insights(ingest_date_start=update_date_start, ingest_date_end=update_date_end)
            ingest_df_insights = ingest_results_insights["ingest_df_final"]
            ingest_status_insights = ingest_results_insights["ingest_status_final"]
            ingest_summary_insights = ingest_results_insights["ingest_summary_final"]
            updated_campaign_ids = set(ingest_df_insights["campaign_id"].dropna().unique())
            if ingest_status_insights == "ingest_succeed_all":
                update_sections_status[update_section_name] = "succeed"
                print(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")                
            elif ingest_status_insights == "ingest_succeed_partial":
                update_sections_status[update_section_name] = "partial"
                print(f"⚠️ [UPDATE] Partially triggered TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")                
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign insights ingestion from {update_date_start} to {update_date_end} with with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.3. Trigger to ingest TikTok Ads campaign metadata
        update_section_name = "[UPDATE] Trigger to ingest TikTok Ads campaign metadata"
        update_section_start = time.time()
        try:
            if updated_campaign_ids:
                print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads campaign metadata for {len(updated_campaign_ids)} campaign_id(s)...")
                ingest_results_metadata = ingest_campaign_metadata(ingest_campaign_ids=list(updated_campaign_ids))
                ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                if ingest_status_metadata == "ingest_succeed_all":
                    update_sections_status[update_section_name] = "succeed"
                    print(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
                elif ingest_status_metadata == "ingest_succeed_partial":
                    update_sections_status[update_section_name] = "partial"
                    print(f"⚠️ [UPDATE] Partially triggered TikTok Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered TikTok Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                                    
                else:
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
            else:
                print("⚠️ [UPDATE] No updates for any campaign_id then TikTok Ads campaign metadata ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any campaign_id then TikTok Ads campaign metadata ingestion is marked as failed.")
                update_sections_status[update_section_name] = "failed"
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.4. Trigger to build staging TikTok Ads campaign insights
        update_section_name = "[UPDATE] Trigger to build staging TikTok Ads campaign insights"
        update_section_start = time.time()
        try:
            if updated_campaign_ids:
                print("🔄 [UPDATE] Triggering to create or overwrite staging TikTok Ads campaign insights table...")
                logging.info("🔄 [UPDATE] Triggering to create or overwrite staging TikTok Ads campaign insights table...")
                staging_results_campaign = staging_campaign_insights()
                staging_status_campaign = staging_results_campaign["staging_status_final"]
                staging_summary_campaign = staging_results_campaign["staging_summary_final"]
                if staging_status_campaign == "staging_succeed_all":
                    update_sections_status[update_section_name] = "succeed"
                    print(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")                    
                elif staging_status_campaign == "staging_failed_partial":
                    update_sections_status[update_section_name] = "partial"
                    print(f"⚠️ [UPDATE] Partially triggered TikTok Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered TikTok Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")                    
                else:
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads campaign insights staging with {staging_summary_campaign['staging_tables_output']}/{staging_summary_campaign['staging_tables_input']} table(s) on {staging_summary_campaign['staging_tables_input']} queried table(s) and {staging_summary_campaign['staging_rows_output']} uploaded row(s) in {staging_summary_campaign['staging_time_elapsed']}s.")                    
            else:
                update_sections_status[update_section_name] = "failed"
                print("⚠️ [UPDATE] No updates for any campaign_id then TikTok Ads campaign insights staging is marked as failed.")
                logging.error("⚠️ [UPDATE] No updates for any campaign_id then TikTok Ads campaign insights staging is marked as failed.")                
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.5. Trigger to materialize TikTok Ads campaign performance table
        update_section_name = "[UPDATE] Trigger to materialize TikTok Ads campaign performance table"
        update_section_start = time.time()
        try:
            if staging_status_campaign in ["staging_succeed_all", "staging_failed_partial"]:
                print("🔄 [UPDATE] Triggering to build materialized TikTok Ads campaign performance table...")
                logging.info("🔄 [UPDATE] Triggering to build materialized TikTok Ads campaign performance table...")               
                mart_results_all = mart_campaign_all()
                mart_status_all = mart_results_all["mart_status_final"]
                mart_summary_all = mart_results_all["mart_summary_final"]                
                if mart_status_all == "mart_succeed_all":
                    update_sections_status[update_section_name] = "succeed"
                    print(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads campaign performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
                elif mart_status_all == "mart_failed_all":
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to complete TikTok Ads campaign performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to complete TikTok Ads campaign performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")                    
            else:
                update_sections_status[update_section_name] = "failed"
                print("⚠️ [UPDATE] No data returned from TikTok Ads campaign insights staging then materialization is skipped.")
                logging.warning("⚠️ [UPDATE] No data returned from TikTok Ads campaign insights staging then materialization is skipped.")                
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.1.6. Summarize update result(s) for TikTok Ads campaign performance
    finally:
        update_time_total = round(time.time() - update_time_start, 2)
        print("\n📊 [UPDATE] TIKTOK ADS CAMPAIGN PERFORMANCE UPDATE SUMMARY")
        print("=" * 110)
        print(f"{'Step':<80} | {'Status':<10} | {'Time (s)'}")
        print("-" * 110)
        summary_map = {
            "[UPDATE] Trigger to ingest TikTok Ads campaign insights": "ingest_results_insights",
            "[UPDATE] Trigger to ingest TikTok Ads campaign metadata": "ingest_results_metadata",
            "[UPDATE] Trigger to build staging TikTok Ads campaign insights": "staging_results_campaign",
            "[UPDATE] Trigger to materialize TikTok Ads campaign performance table": "mart_results_all",
        }
        locals_dict = locals()
        for update_step_name, update_step_status in update_sections_status.items():
            summary_obj = None
            if update_step_name in summary_map and summary_map[update_step_name] in locals_dict:
                summary_obj = locals_dict[summary_map[update_step_name]]
            nested_summary = None
            if summary_obj and isinstance(summary_obj, dict):
                for k in summary_obj.keys():
                    if k.endswith("_summary_final"):
                        nested_summary = summary_obj[k]
                        break
            candidate_dict = (nested_summary or summary_obj or {})
            step_time = None
            for key in ["ingest_time_elapsed", "staging_time_elapsed", "mart_time_elapsed"]:
                if key in candidate_dict and candidate_dict[key] is not None:
                    step_time = candidate_dict[key]
                    break
            if step_time is None:
                step_time = "-"
            time_str = "-" if step_time == "-" else f"{step_time:>8.2f}"
            print(f"• {update_step_name:<76} | {update_step_status:<10} | {time_str}")
            if nested_summary:
                for detail_key in [k for k in nested_summary.keys() if k.endswith("_sections_detail")]:
                    detail_dict = nested_summary[detail_key]
                    for idx, (sub_step, sub_info) in enumerate(detail_dict.items(), start=1):
                        sub_status = sub_info.get("status", "-")
                        sub_time_section = sub_info.get("time", 0.0)
                        sub_loop_time = sub_info.get("loop_time", 0.0)
                        sub_total = round(sub_time_section + sub_loop_time, 2)
                        print(f"    {idx:>2}. {sub_step:<70} | {sub_status:<10} | {sub_total:>8.2f}")
        print("-" * 110)
        print(f"{'Total execution time':<80} | {'-':<10} | {update_time_total:>8.2f}s")
        print("=" * 110)

# 1.2. Update TikTok Ads ad insights
def update_ad_insights(update_date_start: str, update_date_end: str):
    print(f"🚀 [UPDATE] Starting to update TikTok Ads ad insights from {update_date_start} to {update_date_end}...")
    logging.info(f"🚀 [UPDATE] Starting to update TikTok Ads ad insights from {update_date_start} to {update_date_end}...")

    # 1.2.1. Start timing TikTok Ads ad insights update
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")
    update_time_start = time.time()
    update_sections_status = {}
    update_sections_time = {}
    print(f"🔍 [UPDATE] Proceeding to update TikTok Ads ad insights from {update_date_start} to {update_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [UPDATE] Proceeding to update TikTok Ads ad insights from {update_date_start} to {update_date_end} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.2.2. Trigger to ingest TikTok Ads ad insights
        update_section_name = "[UPDATE] Trigger to ingest TikTok Ads ad insights"
        update_section_start = time.time()
        try:
            print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end}...")
            logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end}...")
            ingest_results_insights = ingest_ad_insights(ingest_date_start=update_date_start, ingest_date_end=update_date_end)
            ingest_df_insights = ingest_results_insights["ingest_df_final"]
            ingest_status_insights = ingest_results_insights["ingest_status_final"]
            ingest_summary_insights = ingest_results_insights["ingest_summary_final"]
            update_ad_ids = set(ingest_df_insights["ad_id"].dropna().unique())
            if ingest_status_insights == "ingest_succeed_all":
                update_sections_status[update_section_name] = "succeed"
                print(f"✅ [UPDATE] Successfully triggered TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")                
            elif ingest_status_insights == "ingest_succeed_partial":
                update_sections_status[update_section_name] = "partial"
                print(f"⚠️ [UPDATE] Partially triggered TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.warning(f"⚠️ [UPDATE] Partially triggered TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")                
            else:
                update_sections_status[update_section_name] = "failed"
                print(f"❌ [UPDATE] Failed to trigger TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
                logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads ad insights ingestion from {update_date_start} to {update_date_end} with {ingest_summary_insights['ingest_dates_output']}/{ingest_summary_insights['ingest_dates_input']} ingested day(s) and {ingest_summary_insights['ingest_rows_output']} ingested row(s) in {ingest_summary_insights['ingest_time_elapsed']}s.")
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.3. Trigger to ingest TikTok Ads ad metadata
        update_section_name = "[UPDATE] Trigger to ingest TikTok Ads ad metadata"
        update_section_start = time.time()
        try:
            if update_ad_ids:
                print(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad metadata for {len(update_ad_ids)} ad_id(s)...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok Ads ad metadata for {len(update_ad_ids)} ad_id(s)...")
                ingest_results_metadata = ingest_ad_metadata(ingest_ad_ids=list(update_ad_ids))
                ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                if ingest_status_metadata == "ingest_succeed_all":
                    update_sections_status[update_section_name] = "partial"
                    print(f"✅ [UPDATE] Successfully triggered TikTok Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
                elif ingest_status_metadata == "ingest_succeed_partial":
                    update_sections_status[update_section_name] = "partial"
                    print(f"⚠️ [UPDATE] Partially triggered TikTok Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered TikTok Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
                else:
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to trigger TikTok Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads ad metadata ingestion with {ingest_summary_metadata['ingest_rows_output']}/{ingest_summary_metadata['ingest_rows_input']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
            else:
                update_sections_status[update_section_name] = "failed"
                print("⚠️ [UPDATE] No updates for any ad_id then TikTok Ads ad metadata ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any ad_id then TikTok Ads ad metadata ingestion is marked as failed.")                
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.4. Trigger to ingest TikTok Ads ad creative
        update_section_name = "[UPDATE] Trigger to ingest TikTok Ads ad creative"
        update_section_start = time.time()
        try:
            if update_ad_ids:
                print(f"🔄 [UPDATE] Triggering to ingest TikTok ad creative...")
                logging.info(f"🔄 [UPDATE] Triggering to ingest TikTok ad creative...")
                ingest_results_metadata = ingest_ad_creative()
                ingest_status_metadata = ingest_results_metadata["ingest_status_final"]
                ingest_summary_metadata = ingest_results_metadata["ingest_summary_final"]
                if ingest_status_metadata == "ingest_succeed_all":
                    update_sections_status[update_section_name] = "succeed"
                    print(f"✅ [UPDATE] Successfully triggered TikTok Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered TikTok Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
                else:
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to trigger TikTok Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger TikTok Ads ad creative ingestion with {ingest_summary_metadata['ingest_rows_output']} ingested row(s) in {ingest_summary_metadata['ingest_time_elapsed']}s.")                    
            else:
                update_sections_status[update_section_name] = "failed"
                print("⚠️ [UPDATE] No updates for any ad_id then TikTok Ads ad creative ingestion is marked as failed.")
                logging.warning("⚠️ [UPDATE] No updates for any ad_id then TikTok Ads ad creative ingestion is marked as failed.")                
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.5. Trigger to build staging TikTok Ads ad insights
        update_section_name = "[UPDATE] Trigger to build staging TikTok Ads ad insights"
        update_section_start = time.time()
        try:
            if update_ad_ids:
                print("🔄 [UPDATE] Triggering to create or overwrite staging TikTok Ads ad insights table...")
                logging.info("🔄 [UPDATE] Triggering to create or overwrite staging TikTok Ads ad insights table...")
                staging_results_ad = staging_ad_insights()
                staging_status_ad = staging_results_ad["staging_status_final"]
                staging_summary_ad = staging_results_ad["staging_summary_final"]
                if staging_status_ad == "staging_succeed_all":
                    update_sections_status[update_section_name] = "succeed"
                    print(f"✅ [UPDATE] Successfully triggered triggered Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully triggered triggered Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")                    
                elif staging_status_ad == "staging_failed_partial":
                    update_sections_status[update_section_name] = "partial"
                    print(f"⚠️ [UPDATE] Partially triggered triggered Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    logging.warning(f"⚠️ [UPDATE] Partially triggered triggered Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")                    
                else:
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to trigger triggered Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to trigger triggered Ads ad insights staging with {staging_summary_ad['staging_tables_output']}/{staging_summary_ad['staging_tables_input']} table(s) on {staging_summary_ad['staging_tables_input']} queried table(s) and {staging_summary_ad['staging_rows_output']} uploaded row(s) in {staging_summary_ad['staging_time_elapsed']}s.")                    
            else:
                update_sections_status[update_section_name] = "failed"
                print("⚠️ [UPDATE] No updates for any ad_id then triggered Ads ad insights staging is marked as failed.")
                logging.error("⚠️ [UPDATE] No updates for any ad_id then triggered Ads ad insights staging is marked as failed.")                
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.6. Trigger to materialize TikTok Ads creative performance table
        update_section_name = "[UPDATE] Trigger to materialize TikTok Ads creative performance table"
        update_section_start = time.time()
        try:
            if staging_status_ad in ["staging_succeed_all", "staging_failed_partial"]:
                print("🔄 [UPDATE] Triggering to build materialized TikTok Ads creative performance table...")
                logging.info("🔄 [UPDATE] Triggering to build materialized TikTok Ads creative performance table...")
                mart_results_all = mart_creative_all()
                mart_status_all = mart_results_all["mart_status_final"]
                mart_summary_all = mart_results_all["mart_summary_final"]                
                if mart_status_all == "mart_succeed_all":
                    update_sections_status[update_section_name] = "succeed"
                    print(f"✅ [UPDATE] Successfully completed TikTok Ads creative performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.info(f"✅ [UPDATE] Successfully completed TikTok Ads creative performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
                elif mart_status_all == "mart_failed_all":
                    update_sections_status[update_section_name] = "failed"
                    print(f"❌ [UPDATE] Failed to complete TikTok Ads creative performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
                    logging.error(f"❌ [UPDATE] Failed to complete TikTok Ads creative performance materialization with {mart_summary_all['mart_rows_output']} in {mart_summary_all['mart_time_elapsed']}s.")
            else:
                update_sections_status[update_section_name] = "failed"
                print("⚠️ [UPDATE] No data returned from TikTok Ads ad insights staging then materialization is skipped.")
                logging.warning("⚠️ [UPDATE] No data returned from TikTok Ads ad insights staging then materialization is skipped.")                
        finally:
            update_sections_time[update_section_name] = round(time.time() - update_section_start, 2)

    # 1.2.8. Summarize update results for TikTok Ads ad insights pipeline
    finally:
        update_time_total = round(time.time() - update_time_start, 2)
        print("\n📊 [UPDATE] TIKTOK ADS CREATIVE PERFORMANCE SUMMARY")
        print("=" * 120)
        print(f"{'Step':<80} | {'Status':<10} | {'Time (s)'}")
        print("-" * 120)
        summary_map = {
            "[UPDATE] Trigger to ingest TikTok Ads ad insights": "ingest_results_insights",
            "[UPDATE] Trigger to ingest TikTok Ads ad metadata": "ingest_results_metadata",
            "[UPDATE] Trigger to ingest TikTok Ads ad creative": "ingest_results_metadata",
            "[UPDATE] Trigger to build staging TikTok Ads ad insights": "staging_results_ad",
            "[UPDATE] Trigger to materialize TikTok Ads creative performance table": "mart_results_all",
        }
        locals_dict = locals()
        for update_step_name, update_step_status in update_sections_status.items():
            summary_obj = None
            if update_step_name in summary_map and summary_map[update_step_name] in locals_dict:
                summary_obj = locals_dict[summary_map[update_step_name]]
            nested_summary = None
            if summary_obj and isinstance(summary_obj, dict):
                for k in summary_obj.keys():
                    if k.endswith("_summary_final"):
                        nested_summary = summary_obj[k]
                        break
            candidate_dict = (nested_summary or summary_obj or {})
            step_time = None
            for key in ["ingest_time_elapsed", "staging_time_elapsed", "mart_time_elapsed"]:
                if key in candidate_dict and candidate_dict[key] is not None:
                    step_time = candidate_dict[key]
                    break
            time_str = "-" if step_time is None else f"{step_time:>8.2f}"
            print(f"• {update_step_name:<76} | {update_step_status:<10} | {time_str}")
            if nested_summary:
                for detail_key in [k for k in nested_summary.keys() if k.endswith("_sections_detail")]:
                    detail_dict = nested_summary[detail_key]
                    for idx, (sub_step, sub_info) in enumerate(detail_dict.items(), start=1):
                        sub_status = sub_info.get("status", "-")
                        sub_time_section = sub_info.get("time", 0.0)
                        sub_loop_time = sub_info.get("loop_time", 0.0)
                        sub_total = round(sub_time_section + sub_loop_time, 2)
                        print(f"    {idx:>2}. {sub_step:<70} | {sub_status:<10} | {sub_total:>8.2f}")
        print("-" * 120)
        print(f"{'Total execution time':<80} | {'-':<10} | {update_time_total:>8.2f}s")
        print("=" * 120)  