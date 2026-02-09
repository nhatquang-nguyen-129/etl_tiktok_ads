import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))
sys.stdout.reconfigure(encoding="utf-8")

from datetime import datetime, timedelta
import pandas as pd
import time

from etl.extract_ad_insights import extract_ad_insights
from etl.extract_ad_metadata import extract_ad_metadata
from etl.extract_ad_creative import extract_ad_creative
from etl.extract_campaign_metadata import extract_campaign_metadata
from etl.transform_ad_insights import transform_ad_insights
from etl.transform_ad_metadata import transform_ad_metadata
from etl.transform_campaign_metadata import transform_campaign_metadata
from etl.load_ad_insights import load_ad_insights
from etl.load_ad_metadata import load_ad_metadata
from etl.load_ad_creative import load_ad_creative
from etl.load_campaign_metadata import load_campaign_metadata

from dbt.run import dbt_tiktok_ads

COMPANY = os.getenv("COMPANY")
PROJECT = os.getenv("PROJECT")
DEPARTMENT = os.getenv("DEPARTMENT")
ACCOUNT = os.getenv("ACCOUNT")
MODE = os.getenv("MODE")

def dags_ad_insights(
    *,
    access_token: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
):
    print(
        "🔄 [DAGS] Trigger to update TikTok Ads ad insights with advertiser_id "
        f"{advertiser_id} from "
        f"{start_date} to "
        f"{end_date}..."
    )

# ETL for TikTok Ads ad insights
    DAGS_INSIGHTS_ATTEMPTS = 3
    DAGS_INSIGHTS_COOLDOWN = 60

    dags_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    dags_end_date   = datetime.strptime(end_date, "%Y-%m-%d").date()

    total_ad_ids: set[str] = set()

    while dags_start_date <= dags_end_date:
        dags_split_date = dags_start_date.strftime("%Y-%m-%d")

        for attempt in range(1, DAGS_INSIGHTS_ATTEMPTS + 1):

    # Extract            
            try:
                print(
                    "🔄 [DAGS] Trigger to extract TikTok Ads ad insights from advertiser_id "
                    f"{advertiser_id} at "
                    f"{dags_split_date} for "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempts..."
                )

                insights = extract_ad_insights(
                    access_token=access_token,
                    advertiser_id=advertiser_id,
                    start_date=dags_split_date,
                    end_date=dags_split_date,
                )

                if insights.empty:
                    print(
                        "⚠️ [DAGS] No TikTok Ads ad insights returned from advertiser_id "
                        f"{advertiser_id} then DAG execution "
                        f"{dags_split_date} will be skipped."
                    )
                    break

    # Transform
                print(
                    "🔄 [DAGS] Trigger to transform TikTok Ads ad insights from advertiser_id "
                    f"{advertiser_id} with "
                    f"{dags_split_date} for "
                    f"{len(insights)} row(s)..."
                )

                insights = transform_ad_insights(insights)

    # Load
                year  = pd.to_datetime(insights["date"].iloc[0]).year
                month = pd.to_datetime(insights["date"].iloc[0]).month

                daily_ad_ids = set(insights["ad_id"].dropna().unique())
                total_ad_ids.update(daily_ad_ids)

                _ad_insights_direction = (
                    f"{PROJECT}."
                    f"{COMPANY}_dataset_tiktok_api_raw."
                    f"{COMPANY}_table_tiktok_{DEPARTMENT}_{ACCOUNT}_ad_m{month:02d}{year}"
                )              

                print(
                    "🔄 [DAGS] Trigger to load TikTok Ads ad insights from advertiser_id "
                    f"{advertiser_id} for "
                    f"{dags_split_date} to direction "
                    f"{_ad_insights_direction}..."
                )

                load_ad_insights(
                    df=insights,
                    direction=_ad_insights_direction,
                )

                break

            except Exception as e:
                retryable = getattr(e, "retryable", False)
                print(
                    "⚠️ [DAGS] Failed to extract TikTok Ads ad insights for "
                    f"{dags_split_date} in "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempts due to "
                    f"{e}."
                )

                if not retryable:
                    raise RuntimeError(
                        "❌ [DAGS] Failed to extract TikTok Ads ad insights for "
                        f"{dags_split_date} due to unexpected error then DAG execution will be aborting."
                    ) from e

                if attempt == DAGS_INSIGHTS_ATTEMPTS:
                    raise RuntimeError(
                        "❌ [DAGS] Failed to extract TikTok Ads ad insights for "
                        f"{dags_split_date} in "
                        f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempts due to exceeded attempt limit then DAG execution will be aborting."
                    ) from e

                wait_to_retry = 60 + (attempt - 1) * 30
                
                print(
                    "🔄 [DAGS] Waiting "
                    f"{wait_to_retry} second(s) before retrying TikTok Ads API "
                    f"{attempt}/{DAGS_INSIGHTS_ATTEMPTS} attempts..."
                )

                time.sleep(wait_to_retry)

        dags_start_date += timedelta(days=1)
        
        if dags_start_date <= dags_end_date:
            print(
                "🔄 [DAGS] Waiting "
                f"{DAGS_INSIGHTS_COOLDOWN} second(s) cooldown before processing next date of TikTok Ads ad insights..."
            )

            time.sleep(DAGS_INSIGHTS_COOLDOWN)

# ETL for TikTok Ads ad metadata
    DAGS_AD_ATTEMPTS = 3
    
    if not total_ad_ids:
        print(
            "⚠️ [DAGS] No TikTok Ads ad_id appended for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        return
    
    remaining_ad_ids = list(total_ad_ids)
    dfs_ad_metadata = []

    for attempt in range(1, DAGS_AD_ATTEMPTS + 1):
        print(
            "🔄 [DAGS] Trigger to extract TikTok Ads ad metadata for "
            f"{len(remaining_ad_ids)} ad_id(s) in "
            f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
        )

    # Extract
        df_ad_metadata = extract_ad_metadata(
            access_token=access_token,
            advertiser_id=advertiser_id,
            ad_ids=remaining_ad_ids,
        )

        if not df_ad_metadata.empty:
            dfs_ad_metadata.append(df_ad_metadata)

        failed_ad_ids = getattr(df_ad_metadata, "failed_ad_ids", [])
        retryable = getattr(df_ad_metadata, "retryable", False)

        if not failed_ad_ids:
            print(
                "✅ [DAGS] Successfully triggered to extract TikTok Ads ad metadata with "
                f"{len(set(pd.concat(dfs_ad_metadata)["ad_id"].dropna()))}/{len(remaining_ad_ids)} row(s)."
            )
            break

        if not retryable:
            print(
                "❌ [DAGS] Failed to extract TikTok Ads ad metadata for "
                f"{len(remaining_ad_ids)} ad_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            break

        if attempt == DAGS_AD_ATTEMPTS:
            print(
                "❌ [DAGS] Failed to extract TikTok Ads ad metadata for "
                f"{len(remaining_ad_ids)} ad_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            break

        remaining_ad_ids = failed_ad_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        print(
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying TikTok Ads API "
                f"{attempt}/{DAGS_AD_ATTEMPTS} attempt(s)..."
            )
        
        time.sleep(wait_to_retry)

    df_ad_metadatas = pd.concat(dfs_ad_metadata, ignore_index=True)

    # Transform
    print(
        "🔄 [DAGS] Trigger to transform TikTok Ads campaign metadata for "
        f"{len(df_ad_metadatas)} row(s)..."
    )

    df_ad_metadatas = transform_ad_metadata(df_ad_metadatas)

    # Load    
    _ad_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_tiktok_api_raw."
        f"{COMPANY}_table_tiktok_{DEPARTMENT}_{ACCOUNT}_ad_metadata"
    )
    
    print(
        "🔄 [DAGS] Trigger to load TikTok Ads ad metadata for "       
        f"{len(df_ad_metadatas)} row(s) for "
        f"{_ad_metadata_direction}..."
    )

    load_ad_metadata(
        df=df_ad_metadatas,
        direction=_ad_metadata_direction,
    )

# ETL for TikTok Ads ad creative
    DAGS_CREATIVE_ATTEMPTS = 3

    dfs_ad_creative = []

    for attempt in range(1, DAGS_CREATIVE_ATTEMPTS + 1):
        print(
            "🔄 [DAGS] Trigger to extract TikTok Ads ad creative for advertiser_id "
            f"{advertiser_id} "
            f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempts..."
        )

    # Extract     
        try:
            df_ad_creative = extract_ad_creative(
                access_token=access_token,
                advertiser_id=advertiser_id,
            )

            if not df_ad_creative.empty:
                dfs_ad_creative.append(df_ad_creative)

            break

        except Exception as e:
            retryable = getattr(e, "retryable", False)

            print(
                "⚠️ [DAGS] Failed to extract TikTok Ads ad creative for advertiser_id "
                f"{advertiser_id} in "
                f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempts due to "
                f"{e}."
            )

            if not retryable:
                raise RuntimeError(
                    "❌ [DAGS] Failed to extract TikTok Ads ad creative due to non-retryable error then DAG execution will be aborting."
                ) from e

            if attempt == DAGS_CREATIVE_ATTEMPTS:
                raise RuntimeError(
                    "❌ [DAGS] Failed to extract TikTok Ads ad creative due to exceeded attempt limit then DAG execution will be aborting."
                ) from e

            wait_to_retry = 60 + (attempt - 1) * 30

            print(
                "🔄 [DAGS] Waiting "
                f"{wait_to_retry} second(s) before retrying TikTok Ads API "
                f"{attempt}/{DAGS_CREATIVE_ATTEMPTS} attempts..."
            )

            time.sleep(wait_to_retry)

    df_ad_creatives = pd.concat(dfs_ad_creative, ignore_index=True)

    # Transform

        # Nothing to transform with TikTok Ads ad creative

    # Load
    _ad_creative_direction = (
            f"{PROJECT}."
            f"{COMPANY}_dataset_tiktok_api_raw."
            f"{COMPANY}_table_tiktok_{DEPARTMENT}_{ACCOUNT}_ad_creative"
        )

    print(
        "🔄 [DAGS] Trigger to load TikTok Ads ad creative for "
        f"{len(df_ad_creatives)} row(s) to "
        f"{_ad_creative_direction}..."        
    )    

    load_ad_creative(
        df=df_ad_creatives,
        direction=_ad_creative_direction,
    )

# ETL for TikTok Ads campaign metadata
    DAGS_CAMPAIGN_ATTEMPTS = 3

    total_campaign_ids = set(df_ad_metadatas["campaign_id"].dropna().unique())

    if not total_campaign_ids:
        print(
            "⚠️ [DAGS] No TikTok Ads campaign_id appended for advertiser_id "
            f"{advertiser_id} from "
            f"{start_date} to "
            f"{end_date} then DAG execution will be suspended."
        )
        return
    
    remaining_campaign_ids = list(total_campaign_ids)
    dfs_campaign_metadata = []

    for attempt in range(1, DAGS_CAMPAIGN_ATTEMPTS + 1):
        print(
            "🔄 [DAGS] Trigger to extract TikTok Ads campaign metadata for "
            f"{len(remaining_campaign_ids)} campaign_id(s) in "
            f"{attempt}/{DAGS_CAMPAIGN_ATTEMPTS} attempt(s)..."
        )

    # Extract
        df_campaign_metadata = extract_campaign_metadata(
            access_token=access_token,
            advertiser_id=advertiser_id,
            campaign_ids=remaining_campaign_ids,
        )

        if not df_campaign_metadata.empty:
            dfs_campaign_metadata.append(df_campaign_metadata)

        failed_campaign_ids = getattr(df_campaign_metadata, "failed_campaign_ids", [])
        retryable = getattr(df_campaign_metadata, "retryable", False)

        if not failed_campaign_ids:
            print(
                "✅ [DAGS] Successfully triggered to extract TikTok Ads campaign metadata with "
                f"{len(set(pd.concat(dfs_campaign_metadata)["campaign_id"].dropna()))}/{len(remaining_campaign_ids)} row(s)."
            )
            break

        if not retryable:
            print(
                "❌ [DAGS] Failed to extract TikTok Ads campaign metadata for "
                f"{len(remaining_campaign_ids)} campaign_id(s) due to unexpected non-retryable error then DAG execution will be suspended."
            )
            break

        if attempt == DAGS_CAMPAIGN_ATTEMPTS:
            print(
                "❌ [DAGS] Failed to extract TikTok Ads campaign metadata for "
                f"{len(remaining_campaign_ids)} campaign_id(s) due to exceeded attempt limit then DAG execution will be suspended."
            )
            break

        remaining_campaign_ids = failed_campaign_ids

        wait_to_retry = 60 + (attempt - 1) * 30
        
        print(
            "🔄 [DAGS] Waiting "
            f"{wait_to_retry} second(s) before retrying TikTok Ads API "
                f"{attempt}/{DAGS_CAMPAIGN_ATTEMPTS} attempt(s)..."
            )
        
        time.sleep(wait_to_retry)

    df_campaign_metadatas = pd.concat(dfs_campaign_metadata, ignore_index=True)

    # Transform
    print(
        "🔄 [DAGS] Trigger to transform TikTok Ads campaign metadata for "
        f"{len(df_campaign_metadatas)} row(s)..."
    )

    df_campaign_metadatas = transform_campaign_metadata(df_campaign_metadatas)

    # Load
    _campaign_metadata_direction = (
        f"{PROJECT}."
        f"{COMPANY}_dataset_tiktok_api_raw."
        f"{COMPANY}_table_tiktok_{DEPARTMENT}_{ACCOUNT}_campaign_metadata"
    )  

    print(
        "🔄 [DAGS] Trigger to load TikTok Ads campaign metadata for "
        f"{len(df_campaign_metadatas)} row(s) to"
        f"{_campaign_metadata_direction}..."       
    )

    load_campaign_metadata(
        df=df_campaign_metadatas,
        direction=_campaign_metadata_direction,
    )
   
# Materialization with dbt
    print("🔄 [DAGS] Trigger to materialize TikTok Ads ad insights with dbt...")
    
    dbt_tiktok_ads(
        google_cloud_project=PROJECT,
        select="tag:mart,tag:ad",
    )