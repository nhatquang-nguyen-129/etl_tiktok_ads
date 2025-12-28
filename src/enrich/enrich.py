"""
==================================================================
TIKTOK ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming raw TikTok Ads insights 
into a clean, BigQuery-ready dataset optimized for advanced analytics, 
cross-platform reporting and machine learning applications.

By centralizing enrichment rules, this module ensures transparency, 
consistency, and maintainability across the marketing data pipeline 
to build insight-ready tables.

✔️ Maps optimization_goal to its corresponding business action type  
✔️ Standardizes campaign, ad set and ad-level naming conventions  
✔️ Extracts and normalizes key performance metrics across campaigns  
✔️ Cleans and validates data to ensure schema and field consistency  
✔️ Reduces payload size by removing redundant or raw fields

⚠️ This module focuses only on enrichment and transformation logic.  
It does not handle data fetching, ingestion or staging.
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

# Add Python regular expression operations ultilities for integraton
import re

# Add Python timezone ultilities for integration
import pytz

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# 1. ENRICH TIKTOK INSIGHTS

# 1.1. Enrich TikTok Ads campaign insights
def enrich_campaign_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s)...")   

    # 1.1.1. Start timing the TikTok Ads campaign insights enrichment
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    print(f"🔍 [ENRICH] Proceeding to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s) at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich TikTok Ads campaign insights for {len(enrich_df_input)} row(s) at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.1.2. Validate input for the TikTok Ads campaign insights enrichment
        enrich_section_name = "[ENRICH] Validate input for the TikTok Ads campaign insights enrichment"
        enrich_section_start = time.time()    
        try:
            if enrich_df_input.empty:
                enrich_sections_status[enrich_section_name] = "failed"
                print("⚠️ [ENRICH] Empty TikTok Ads campaign insights provided then enrichment is suspended.")
                logging.warning("⚠️ [ENRICH] Empty TikTok Ads campaign insights provided then enrichment is suspended.")
            else:
                enrich_sections_status[enrich_section_name] = "succeed"
                print("✅ [ENRICH] Successfully validated input for TikTok Ads campaign insights enrichment.")
                logging.info("✅ [ENRICH] Successfully validated input for TikTok Ads campaign insights enrichment.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)
    
    # 1.1.3. Enrich table fields for TikTok Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich table fields for TikTok Ads campaign insights"
        enrich_section_start = time.time()            
        try: 
            print(f"🔍 [ENRICH] Enriching table fields for TikTok Ads campaign insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table fields for TikTok Ads campaign insights with {len(enrich_df_input)} row(s)...")
            enrich_df_table = enrich_df_input.copy()    
            enrich_table_name = enrich_table_id.split(".")[-1]
            enrich_table_convention = re.search(r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_campaign_m\d{6}$",enrich_table_name)            
            enrich_df_table = enrich_df_table.assign(
                enrich_account_platform=enrich_table_convention.group("platform") if enrich_table_convention else "unknown",
                enrich_account_department=enrich_table_convention.group("department") if enrich_table_convention else "unknown",
                enrich_account_name=enrich_table_convention.group("account") if enrich_table_convention else "unknown"
            )            
            enrich_sections_status[enrich_section_name] = "succeed"            
            print(f"✅ [ENRICH] Successfully enriched table fields for TikTok Ads campaign insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table fields for TikTok Ads campaign insights with {len(enrich_df_table)} row(s).")
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table fields for TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table fields for TikTok Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)  

    # 1.1.4. Enrich campaign fields for TikTok Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich campaign fields for TikTok Ads campaign insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching campaign fields for TikTok Ads campaign insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign fields for TikTok Ads campaign insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign = (
                enrich_df_campaign
                .assign(
                    enrich_campaign_objective=lambda df: df["campaign_name"].str.split("_").str[0].fillna("unknown"),
                    enrich_campaign_region=lambda df: df["campaign_name"].str.split("_").str[1].fillna("unknown"),
                    enrich_budget_group=lambda df: df["campaign_name"].str.split("_").str[2].fillna("unknown"),
                    enrich_budget_type=lambda df: df["campaign_name"].str.split("_").str[3].fillna("unknown"),
                    enrich_category_group=lambda df: df["campaign_name"].str.split("_").str[4].fillna("unknown"),
                    enrich_campaign_personnel=lambda df: df["campaign_name"].str.split("_").str[5].fillna("unknown"),
                    enrich_program_track=lambda df: df["campaign_name"].str.split("_").str[7].fillna("unknown"),
                    enrich_program_group=lambda df: df["campaign_name"].str.split("_").str[8].fillna("unknown"),
                    enrich_program_type=lambda df: df["campaign_name"].str.split("_").str[9].fillna("unknown"),
                )
            )       
            enrich_sections_status[enrich_section_name] = "succeed"            
            print(f"✅ [ENRICH] Successfully enriched campaign fields for TikTok Ads campaign insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign fields for TikTok Ads campaign insights with {len(enrich_df_campaign)} row(s).")            
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign fields for TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign fields for TikTok Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.1.5. Enrich date fields for TikTok Ads campaign insights
        enrich_section_name = "[ENRICH] Enrich date fields for TikTok Ads campaign insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching date fields for TikTok Ads campaign insights with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching date fields for TikTok Ads campaign insights with {len(enrich_df_campaign)} row(s)...")
            enrich_df_other = enrich_df_campaign.copy()
            enrich_df_other = enrich_df_other.rename(columns={"stat_time_day": "date_start"})
            enrich_df_other = enrich_df_other.assign(
                date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                year=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y"),
                month=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC),
            ).drop(columns=["date_start"], errors="ignore")
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched date fields for TikTok Ads campaign insights with {len(enrich_df_other)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date fields for TikTok Ads campaign insights with {len(enrich_df_other)} row(s).")           
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date fields for TikTok Ads campaign insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich date fields for TikTok Ads campaign insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2) 

    # 1.1.6. Summarize enrichment results for TikTok campaign insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_other.copy() if not enrich_df_other.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if enrich_sections_failed:
            enrich_status_final = "enrich_failed_all"
            print(f"❌ [ENRICH] Failed to complete TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
        elif enrich_rows_output == enrich_rows_input:
            enrich_status_final = "enrich_succeed_all"
            print(f"🏆 [ENRICH] Successfully completed TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")            
        else:
            enrich_status_final = "enrich_succeed_partial"
            print(f"⚠️ [ENRICH] Partially completed TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.warning(f"⚠️ [ENRICH] Partially completed TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")            
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }
    return enrich_results_final

# 1.2. Enrich TikTok Ads ad insights
def enrich_ad_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:   
    print(f"🚀 [ENRICH] Starting to enrich TikTok Ads ad insights for {len(enrich_df_input)}...")
    logging.info(f"🚀 [ENRICH] Starting to enrich TikTok Ads ad insights for {len(enrich_df_input)}...")        
    
    # 1.2.1. Start timing the TikTok Ads ad insights enrichment
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    print(f"🔍 [ENRICH] Proceeding to enrich TikTok Ads ad insights for {len(enrich_df_input)} row(s) at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich TikTok Ads ad insights for {len(enrich_df_input)} row(s) at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    try:

    # 1.2.2. Validate input for the TikTok Ads ad insights enrichment
        enrich_section_name = "[ENRICH] Validate input for the TikTok Ads ad insights enrichment"
        enrich_section_start = time.time()    
        try:
            if enrich_df_input.empty:
                enrich_sections_status[enrich_section_name] = "failed"
                print("⚠️ [ENRICH] Empty TikTok Ads ad insights provided then enrichment is suspended.")
                logging.warning("⚠️ [ENRICH] Empty TikTok Ads ad insights provided then enrichment is suspended.")
            else:
                enrich_sections_status[enrich_section_name] = "succeed"
                print("✅ [ENRICH] Successfully validated input for TikTok Ads ad insights enrichment.")
                logging.info("✅ [ENRICH] Successfully validated input for TikTok Ads ad insights enrichment.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.3. Enrich table fields for TikTok Ads ad insights
        enrich_section_name = "[ENRICH] Enrich table fields for TikTok Ads ad insights"
        enrich_section_start = time.time()   
        try:
            print(f"🔍 [ENRICH] Enriching table fields for TikTok Ads ad insights with {len(enrich_df_input)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table fields for TikTok Ads ad insights with {len(enrich_df_input)} row(s)...")
            enrich_df_table = enrich_df_input.copy()
            enrich_table_name = enrich_table_id.split(".")[-1]
            enrich_table_convention = re.search(r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_ad_m\d{6}$", enrich_table_name)
            enrich_df_table = enrich_df_table.assign(
                spend=lambda df: pd.to_numeric(df["spend"], errors="coerce").fillna(0),
                enrich_account_platform=enrich_table_convention.group("platform") if enrich_table_convention else None,
                enrich_account_department=enrich_table_convention.group("department") if enrich_table_convention else None,
                enrich_account_name=enrich_table_convention.group("account") if enrich_table_convention else None
            )
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched table fields for TikTok Ads ad insights with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table fields for TikTok Ads ad insights with {len(enrich_df_table)} row(s).")
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table fields for TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table fields for TikTok Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)    

    # 1.2.4. Enrich campaign fields for TikTok Ads ad insights
        enrich_section_name = "[ENRICH] Enrich campaign fields for TikTok Ads ad insights"
        enrich_section_start = time.time()  
        try:
            print(f"🔍 [ENRICH] Enriching campaign fields for TikTok Ads ad insights with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign fields for TikTok Ads ad insights with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign = (
                enrich_df_campaign
                .assign(
                    enrich_campaign_objective=lambda df: df["campaign_name"].str.split("_").str[0].fillna("unknown"),
                    enrich_campaign_region=lambda df: df["campaign_name"].str.split("_").str[1].fillna("unknown"),
                    enrich_budget_group=lambda df: df["campaign_name"].str.split("_").str[2].fillna("unknown"),
                    enrich_budget_type=lambda df: df["campaign_name"].str.split("_").str[3].fillna("unknown"),
                    enrich_category_group=lambda df: df["campaign_name"].str.split("_").str[4].fillna("unknown"),
                    enrich_campaign_personnel=lambda df: df["campaign_name"].str.split("_").str[5].fillna("unknown"),
                    enrich_program_track=lambda df: df["campaign_name"].str.split("_").str[7].fillna("unknown"),
                    enrich_program_group=lambda df: df["campaign_name"].str.split("_").str[8].fillna("unknown"),
                    enrich_program_type=lambda df: df["campaign_name"].str.split("_").str[9].fillna("unknown"),
                )
            )
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched campaign fields for TikTok Ads ad insights with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign fields for TikTok Ads ad insights with {len(enrich_df_campaign)} row(s).")           
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign fields for TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign fields for TikTok Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)  

    # 1.2.5. Enrich adset fields for TikTok Ads ad insights
        enrich_section_name = "[ENRICH] Enrich adset fields for TikTok Ads ad insights"
        enrich_section_start = time.time()         
        try:
            print(f"🔍 [ENRICH] Enriching adset fields for TikTok Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching adset fields for TikTok Ads ad insights with {len(enrich_df_campaign)} row(s)...")
            enrich_df_adset = enrich_df_campaign.copy()
            enrich_df_adset = enrich_df_adset.assign(
                enrich_adset_location=lambda df: df["adgroup_name"].fillna("").str.split("_").str[0].fillna("unknown"),
                enrich_adset_audience=lambda df: df["adgroup_name"].fillna("").str.split("_").str[1].fillna("unknown"),
                enrich_adset_format=lambda df: df["adgroup_name"].fillna("").str.split("_").str[2].fillna("unknown"),
                enrich_adset_strategy=lambda df: df["adgroup_name"].fillna("").str.split("_").str[3].fillna("unknown"),
                enrich_adset_subtype=lambda df: df["adgroup_name"].fillna("").str.split("_").str[4].fillna("unknown")
            )
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched adset fields for TikTok Ads ad insights with {len(enrich_df_adset)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched adset fields for TikTok Ads ad insights with {len(enrich_df_adset)} row(s).")            
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich adset fields for TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich adset fields for TikTok Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.6. Enrich date fields for TikTok Ads ad insights
        enrich_section_name = "[ENRICH] Enrich date fields for TikTok Ads ad insights"
        enrich_section_start = time.time()            
        try:
            print(f"🔍 [ENRICH] Enriching date fields for TikTok Ads ad insights with {len(enrich_df_adset)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching date fields for TikTok Ads ad insights with {len(enrich_df_adset)} row(s)...")
            enrich_df_other = enrich_df_adset.copy()
            enrich_df_other = enrich_df_other.rename(columns={"stat_time_day": "date_start"})
            enrich_df_other = enrich_df_other.assign(
                date=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.floor("D"),
                year=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y"),
                month=lambda df: pd.to_datetime(df["date_start"], errors="coerce", utc=True).dt.strftime("%Y-%m"),
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC),
            ).drop(columns=["date_start"], errors="ignore")
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched date fields for TikTok Ads ad insights with {len(enrich_df_other)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date fields for TikTok Ads ad insights with {len(enrich_df_other)} row(s).")           
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date fields for TikTok Ads ad insights due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich other field(s) for TikTok Ads ad insights due to {e}.")
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.2.7. Summarize enrich results for TikTok ad insights
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_other.copy() if not enrich_df_other.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if enrich_sections_failed:
            enrich_status_final = "enrich_failed_all"
            print(f"❌ [ENRICH] Failed to complete TikTok Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete TikTok Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
        elif enrich_rows_output == enrich_rows_input:
            enrich_status_final = "enrich_succeed_all"
            print(f"🏆 [ENRICH] Successfully completed TikTok Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed TikTok Ads ad insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
        else:
            enrich_status_final = "enrich_succeed_partial"
            print(f"⚠️ [ENRICH] Partially completed TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.warning(f"⚠️ [ENRICH] Partially completed TikTok Ads campaign insights enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")                    
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }    
    return enrich_results_final