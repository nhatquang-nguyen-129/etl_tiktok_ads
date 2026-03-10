import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_ad_creative(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load TikTok Ads ad creative
    ---
    Principles:
        1. No need input ad_ids
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to advertiser_id and ad_id
        4. Use UPSERT mode with temporary table for deduplication
        5. Make internalGoogleBigQueryLoader API call
    ---
    Returns:
        None
    """      

    if df.empty:
        print(
            "⚠️ [LOADER] Empty TikTok Ads ad creative then loading will be suspended."
        )
        return

    print(
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of TikTok Ads ad creative to Google BigQuery table "
        f"{direction}..."
    )
    
    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=[
            "advertiser_id", 
            "video_id"
        ],
        partition=None,
        cluster=[
            "video_id"
        ],
    )