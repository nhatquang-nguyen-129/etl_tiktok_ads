import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_ad_metadata(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load Google Ads ad metadata
    ----------------------
    Workflow:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to advertiser_id and ad_id
        4. Use UPSERT mode with temporary table for deduplication
        5. Make internalGoogleBigQueryLoader API call
    ---------
    Returns:
        None
    """      

    if df.empty:
        msg = ("⚠️ [LOADER] Empty Google Ads campaign metadata Dataframe then loading will be suspended.")
        print(msg)
        logging.warning(msg)
        return

    msg = (
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of Google Ads campaign metadata to Google BigQuery table "
        f"{direction}..."
        )
    
    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=[
            "advertiser_id", 
            "campaign_id"
        ],
        partition=None,
        cluster=[
            "campaign_id"
        ],
    )