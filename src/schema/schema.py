"""
===================================================================
TIKTOK SCHEMA MODULE
-------------------------------------------------------------------
This module provides a centralized definition and management of  
schema structures used throughout the TikTok Ads data pipeline.  
It shares a consistent structure and data type alignment.  

Its main purpose is to validate, enforce, and standardize field 
structures across every pipeline stage to support reliable ETL
execution and seamless data integration.

✔️ Define and store expected field names and data types
✔️ Validate schema integrity before ingestion or transformation  
✔️ Enforce data type consistency across different processing layers  
✔️ Automatically handle missing or mismatched columns  
✔️ Provide schema utilities for debugging and audit logging  

⚠️ This module does not perform data fetching or transformation.  
It serves purely as a utility layer to support schema consistency  
throughout the TikTok Ads ETL process.
===================================================================
"""
# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities for integration
import logging

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# 1. ENSURE SCHEMA FOR GIVEN PYTHON DATAFRAME

# 1.1. Enforce that the given DataFrame contains all required columns with correct datatypes
def enforce_table_schema(schema_df_input: pd.DataFrame, schema_type_mapping: str) -> pd.DataFrame:
    
    # 1.1.1. Start timing the TikTok Ads schema enforcement
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    schema_time_start = time.time()
    schema_sections_status = {}
    schema_sections_time = {}
    print(f"🔍 [SCHEMA] Proceeding to enforce schema for TikTok Ads with {len(schema_df_input)} given row(s) for mapping type {schema_type_mapping} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [SCHEMA] Proceeding to enforce schema for TikTok Ads with {len(schema_df_input)} given row(s) for mapping type {schema_type_mapping} at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    # 1.1.2. Define schema mapping for TikTk Ads data type
    schema_section_name = "[SCHEMA] Define schema mapping for TikTk Ads data type"
    schema_section_start = time.time()    
    schema_types_mapping = {
        "fetch_campaign_metadata": {
            "advertiser_id": str,
            "advertiser_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "objective_type": str,       
            "create_time": "datetime64[ns, UTC]"
        },
        "fetch_ad_metadata": {
            "advertiser_id": str,
            "ad_id": str,
            "ad_name": str,
            "adgroup_id": str,
            "adgroup_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "create_time": "datetime64[ns, UTC]",
            "ad_format": str,
            "optimization_event": str,
            "video_id": str
        },
        "fetch_ad_creative": {
            "advertiser_id": str,
            "video_id": str,
            "video_cover_url": str,
            "preview_url": str,
            "create_time": "datetime64[ns, UTC]"
        },
        "fetch_campaign_insights": {
            "advertiser_id": str,
            "campaign_id": str,
            "stat_time_day": str,
            "result": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "engaged_view_15s": int,
            "purchase": int,
            "complete_payment": int,
            "onsite_total_purchase": int,
            "offline_shopping_events": int,
            "onsite_shopping": int,
            "messaging_total_conversation_tiktok_direct_message": int
        },
        "fetch_ad_insights": {
            "advertiser_id": str,
            "ad_id": str,
            "stat_time_day": str,
            "result": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "engaged_view_15s": int,
            "purchase": int,
            "complete_payment": int,
            "onsite_total_purchase": int,
            "offline_shopping_events": int,
            "onsite_shopping": int,
            "messaging_total_conversation_tiktok_direct_message": int
        },
        "ingest_campaign_metadata": {
            "advertiser_id": str,
            "advertiser_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "objective_type": str,        
            "create_time": "datetime64[ns, UTC]"
        },
        "ingest_ad_metadata": {
            "advertiser_id": str,
            "ad_id": str,
            "ad_name": str,
            "adgroup_id": str,
            "adgroup_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "operation_status": str,
            "create_time": "datetime64[ns, UTC]",
            "ad_format": str,
            "optimization_event": str,
            "video_id": str
        },
        "ingest_ad_creative": {
            "advertiser_id": str,
            "video_id": str,
            "video_cover_url": str,
            "preview_url": str,
            "create_time": "datetime64[ns, UTC]"
        },
        "ingest_campaign_insights": {
            "advertiser_id": str,
            "campaign_id": str,
            "result": str,
            "stat_time_day": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "engaged_view_15s": int,
            "purchase": int,
            "complete_payment": int,
            "onsite_total_purchase": int,
            "offline_shopping_events": int,
            "onsite_shopping": int,
            "messaging_total_conversation_tiktok_direct_message": int,
        },
        "ingest_ad_insights": {
            "advertiser_id": str,
            "ad_id": str,        
            "result": str,
            "stat_time_day": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "engaged_view_15s": int,
            "purchase": int,
            "complete_payment": int,
            "onsite_total_purchase": int,
            "offline_shopping_events": int,
            "onsite_shopping": int,
            "messaging_total_conversation_tiktok_direct_message": int,
        },
        "staging_campaign_insights": {
            
            # Original staging ad fields
            "account_id": str,
            "account_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "delivery_status": str,
            "objective_type": str,
            "result": str,
            "date_start": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "engaged_view_15s": int,
            "purchase": int,
            "complete_payment": int,
            "onsite_total_purchase": int,
            "offline_shopping_events": int,
            "onsite_shopping": int,
            "messaging_total_conversation_tiktok_direct_message": int,
            
            # Enriched dimensions from campaign_name and specific to campaign settings
            "enrich_campaign_objective": str,
            "enrich_campaign_region": str,
            "enrich_campaign_personnel": str,            
            
            # Enriched dimensions from campaign_name and specific to budget classfication
            "enrich_budget_group": str,
            "enrich_budget_type": str,            
            
            # Enriched dimensions from campaign_name and specific to category classification
            "enrich_category_group": str,            
            
            # Enriched dimensions from campaign_name and specific to advertising strategy
            "enrich_program_track": str,
            "enrich_program_group": str,
            "enrich_program_type": str,            
            
            # Standardized time columns
            "date": "datetime64[ns, UTC]",
            "year": str,
            "month": str,
            "last_updated_at": "datetime64[ns, UTC]",            
            
            # Enriched dimensions from table_id and specific to internal company structure
            "enrich_account_platform": str,
            "enrich_account_department": str,
            "enrich_account_name": str
        },
        "staging_ad_insights": {
            
            # Original staging ad fields            
            "account_id": str,
            "ad_id": str,
            "ad_name": str,
            "adset_id": str,
            "adset_name": str,
            "campaign_id": str,
            "campaign_name": str,
            "date_start": str,
            "delivery_status": str,
            "ad_format": str,
            "video_id": str,
            "video_cover_url": str,
            "preview_url": str,
            "optimization_event": str,
            "result": str,
            "spend": float,
            "impressions": int,
            "clicks": int,
            "engaged_view_15s": int,
            "purchase": int,
            "complete_payment": int,
            "onsite_total_purchase": int,
            "offline_shopping_events": int,
            "onsite_shopping": int,
            "messaging_total_conversation_tiktok_direct_message": int,
            
            # Enriched dimensions from campaign_name and specific to campaign settings
            "enrich_campaign_objective": str,
            "enrich_campaign_region": str,
            "enrich_campaign_personnel": str,            
            
            # Enriched dimensions from campaign_name and specific to budget classfication
            "enrich_budget_group": str,
            "enrich_budget_type": str,            
            
            # Enriched dimensions from campaign_name and specific to category classification
            "enrich_category_group": str,            
            
            # Enriched dimensions from campaign_name and specific to advertising strategy
            "enrich_program_track": str,
            "enrich_program_group": str,
            "enrich_program_type": str,            
            
            # Enriched dimensions from adset_name and specific to advertising strategy
            "enrich_adset_strategy": str,
            "enrich_adset_subtype": str,            
            
            # Enriched dimensions from adset_name and specific to targeting
            "enrich_adset_location": str,
            "enrich_adset_audience": str,
            "enrich_adset_format": str,            
            
            # Enriched dimensions from table_id and specific to internal company structure
            "enrich_account_platform": str,
            "enrich_account_department": str,
            "enrich_account_name": str,            
            
            # Standardized time columns
            "date": "datetime64[ns, UTC]",
            "year": str,
            "month": str,
            "last_updated_at": "datetime64[ns, UTC]"
        }
    }
    schema_sections_status[schema_section_name] = "succeed"
    schema_sections_time[schema_section_name] = round(time.time() - schema_section_start, 2)
    
    try:

    # 1.1.3. Validate that the given schema_type_mapping exists
        schema_section_name = "[SCHEMA] Validate that the given schema_type_mapping exists"
        schema_section_start = time.time()            
        try:
            if schema_type_mapping not in schema_types_mapping:
                schema_sections_status[schema_section_name] = "failed"
                print(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for TikTok Ads then enforcement is suspended.")
                logging.error(f"❌ [SCHEMA] Failed to validate schema type {schema_type_mapping} for TikTok Ads then enforcement is suspended.")
            else:
                schema_columns_expected = schema_types_mapping[schema_type_mapping]
                schema_sections_status[schema_section_name] = "succeed"
                print(f"✅ [SCHEMA] Successfully validated schema type {schema_type_mapping} for TikTok Ads.")
                logging.info(f"✅ [SCHEMA] Successfully validated schema type {schema_type_mapping} for TikTok Ads.")
        finally:
            schema_sections_time[schema_section_name] = round(time.time() - schema_section_start, 2)

    # 1.1.4. Enforce schema columns for TikTok Ads
        schema_section_name = "[SCHEMA] Enforce schema columns for TikTok Ads"
        schema_section_start = time.time()              
        try:
            print(f"🔄 [SCHEMA] Enforcing schema for TikTok Ads with schema type {schema_type_mapping}...")
            logging.info(f"🔄 [SCHEMA] Enforcing schema for TikTok Ads with schema type {schema_type_mapping}...")
            schema_df_enforced = schema_df_input.copy()            
            for schema_column_expected, schema_data_type in schema_columns_expected.items():
                if schema_column_expected not in schema_df_enforced.columns: 
                    schema_df_enforced[schema_column_expected] = pd.NA               
                try:
                    if schema_data_type == int:
                        schema_df_enforced[schema_column_expected] = pd.to_numeric(
                            schema_df_enforced[schema_column_expected].astype(str).str.replace(",", "."), errors="coerce"
                        ).fillna(0).astype(int)

                    elif schema_data_type == float:
                        schema_df_enforced[schema_column_expected] = pd.to_numeric(
                            schema_df_enforced[schema_column_expected].astype(str).str.replace(",", "."), errors="coerce"
                        ).fillna(0.0).astype(float)
                    elif schema_data_type == "datetime64[ns, UTC]":
                        schema_df_enforced[schema_column_expected] = pd.to_datetime(
                            schema_df_enforced[schema_column_expected], errors="coerce", utc=True
                        )
                    elif schema_data_type == str:
                        schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected].astype(str).fillna("")
                    else:
                        schema_df_enforced[schema_column_expected] = schema_df_enforced[schema_column_expected]
                except Exception as e:
                    print(f"⚠️ [SCHEMA] Failed to coerce column {schema_column_expected} to {schema_data_type} due to {e}.")
                    logging.warning(f"⚠️ [SCHEMA] Failed to coerce column {schema_column_expected} to {schema_data_type} due to {e}.")
            schema_df_enforced = schema_df_enforced[list(schema_columns_expected.keys())]       
            schema_sections_status[schema_section_name] = "succeed"
            print(f"✅ [SCHEMA] Successfully enforced schema for TikTok Ads with {len(schema_df_enforced)} row(s) and schema type {schema_type_mapping}.")
            logging.info(f"✅ [SCHEMA] Successfully enforced schema for TikTok Ads with {len(schema_df_enforced)} row(s) and schema type {schema_type_mapping}.")
        except Exception as e:
            schema_sections_status[schema_section_name] = "failed"
            print(f"❌ [SCHEMA] Failed to enforce schema for TikTok Ads with schema type {schema_type_mapping} due to {e}.")
            logging.error(f"❌ [SCHEMA] Failed to enforce schema for TikTok Ads with schema type {schema_type_mapping} due to {e}.")
        finally:
            schema_sections_time[schema_section_name] = round(time.time() - schema_section_start, 2)       

    # 1.1.5. Summarize schema enforcement results for TikTok Ads
    finally:
        schema_time_elapsed = round(time.time() - schema_time_start, 2)
        schema_df_final = schema_df_enforced.copy() if "schema_df_enforced" in locals() and not schema_df_enforced.empty else pd.DataFrame()        
        schema_sections_total = len(schema_sections_status)
        schema_sections_succeed = [k for k, v in schema_sections_status.items() if v == "succeed"]
        schema_sections_failed = [k for k, v in schema_sections_status.items() if v == "failed"]        
        schema_rows_input = len(schema_df_input)
        schema_rows_output = len(schema_df_final)        
        schema_sections_summary = list(dict.fromkeys(
            list(schema_sections_status.keys()) +
            list(schema_sections_time.keys())
        ))
        schema_sections_detail = {
            schema_section_summary: {
                "status": schema_sections_status.get(schema_section_summary, "unknown"),
                "time": round(schema_sections_time.get(schema_section_summary, 0.0), 2),
            }
            for schema_section_summary in schema_sections_summary
        }         
        if schema_sections_failed:
            schema_status_final = "schema_failed_all"
            print(f"❌ [SCHEMA] Failed to complete TikTok Ads schema enforcement with {schema_rows_output}/{schema_rows_input} enforced row(s) due to section(s) {', '.join(schema_sections_failed)} in {schema_time_elapsed}s.")
            logging.error(f"❌ [SCHEMA] Failed to complete TikTok Ads schema enforcement with {schema_rows_output}/{schema_rows_input} enforced row(s) due to section(s) {', '.join(schema_sections_failed)} in {schema_time_elapsed}s.")
        elif schema_rows_output == schema_rows_input:
            schema_status_final = "schema_succeed_all"
            print(f"🏆 [SCHEMA] Successfully completed TikTok Ads schema enforcement with {schema_rows_output}/{schema_rows_input} enforced row(s) in {schema_time_elapsed}s.")
            logging.info(f"🏆 [SCHEMA] Successfully completed TikTok Ads schema enforcement with {schema_rows_output}/{schema_rows_input} enforced row(s) in {schema_time_elapsed}s.")            
        else:
            schema_status_final = "schema_succeed_partial"
            print(f"⚠️ [SCHEMA] Partially completed TikTok Ads schema enforcement with {schema_rows_output}/{schema_rows_input} enforced row(s) in {schema_time_elapsed}s.")
            logging.warning(f"⚠️ [SCHEMA] Partially completed TikTok Ads schema enforcement with {schema_rows_output}/{schema_rows_input} enforced row(s) in {schema_time_elapsed}s.")
        schema_results_final = {
            "schema_df_final": schema_df_final,
            "schema_status_final": schema_status_final,
            "schema_summary_final": {
                "schema_time_elapsed": schema_time_elapsed,
                "schema_sections_total": schema_sections_total,
                "schema_sections_succeed": schema_sections_succeed,
                "schema_sections_failed": schema_sections_failed,
                "schema_sections_detail": schema_sections_detail,
                "schema_rows_input": schema_rows_input,
                "schema_rows_output": schema_rows_output,
            },
        }    
    return schema_results_final