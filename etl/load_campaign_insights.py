import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_campaign_insights(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load TikTok Ads campaign insights
    ----------------------
    Workflow:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to date
        4. Use UPSERT mode with parameterized query for deduplication
        5. Make internalGoogleBigQueryLoader API call
    ---------
    Returns:
        None
    """    

    if df.empty:
        print("⚠️ [LOADER] Empty TikTok Ads campaign insights Dataframe then loading will be suspended.")
        return

    print(
        "🔄 [LOADER] Triggering to load "
        f"{len(df)} row(s) of TikTok Ads campaign insights to Google BigQuery table "
        f"{direction}..."
        )

    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=["date"],
        partition={
            "field": "date"
        },
        cluster=[
            "campaign_id"
        ],
    )