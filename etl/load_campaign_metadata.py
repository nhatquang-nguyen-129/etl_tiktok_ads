import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_campaign_metadata(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load TikTok Ads campaign metadata
    ----------------------
    Workflow:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to advertiser_id and campaign_id
        4. Use UPSERT mode with temporary table for deduplication
        5. Make internalGoogleBigQueryLoader API call
    ---------
    Returns:
        None
    """      

    if df.empty:
        msg = ("⚠️ [LOADER] Empty TikTok Ads campaign metadata Dataframe then loading will be suspended.")
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of TikTok Ads campaign metadata to Google BigQuery table "
        f"{direction}..."
        )
    
    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=[
            "advertiser_id", 
            "ad_id"
        ],
        partition=None,
        cluster=[
            "ad_id"
        ],
    )