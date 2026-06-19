import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_campaign_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform TikTok Ads campaign metadata
    ---
    Principles:
        1. Validate input Dataframe
        2. Validate required schema columns
        3. Create copy to prevent side effects
        4. Parse structured naming convention
        5. Enrich Dataframe
    ---
    Returns:
        1. DataFrame:
            Enforced campaign metadata records
    """

    # Validate input
    print(
        "🔄 [TRANSFORM] Validating column(s) for "
        f"{len(df)} row(s) of TikTok Ads campaign metadata..."
    )

    if df.empty:

        raise ValueError(
            "❌ [TRANSFORM] Failed to validate column(s) for TikTok Ads campaign metadata due to empty input DataFrame."
        )

    required_cols = {
        "advertiser_id",
        "campaign_id",
        "campaign_name"
        }
    
    actual_cols = {
        str(col).strip()
        for col in df.columns
    }

    missing_cols = required_cols - actual_cols

    extra_cols = actual_cols - required_cols

    print(
        "✅ [TRANSFORM] Successfully validated DataFrame for TikTok Ads campaign metadata with "
        f"{df.shape} shape with total column(s) "
        f"{len(actual_cols)}/{len(required_cols)} total column including "
        f"{len(missing_cols)} missing column(s) and "
        f"{len(extra_cols)} extra column(s)."
    )

    if missing_cols:

        raise ValueError(
            "❌ [TRANSFORM] Failed to transform validated DataFrame for TikTok Ads campaign metadata due to missing required column(s) "
            f"{sorted(missing_cols)}"
        )

    df = df.copy()
    
    df["platform"] = "TikTok"
    
    df = df.rename(columns={"objective": "result_type"})
    
    df = df.assign(
        objective=df["campaign_name"].fillna("").str.split("|").str[0].fillna("unknown"),
        budget_group=df["campaign_name"].fillna("").str.split("|").str[1].fillna("unknown"),        
        region=df["campaign_name"].fillna("").str.split("|").str[2].fillna("unknown"),
        category_level_1=df["campaign_name"].fillna("").str.split("|").str[3].fillna("unknown"),
        optimization=df["campaign_name"].fillna("").str.split("|").str[6].fillna("unknown"),
        track=df["campaign_name"].fillna("").str.split("|").str[7].fillna("unknown"),
        pillar=df["campaign_name"].fillna("").str.split("|").str[8].fillna("unknown"),
        group=df["campaign_name"].fillna("").str.split("|").str[9].fillna("unknown"),
    )

    print(
        "✅ [TRANSFORM] Successfully transformed TikTok Ads campaign metadata with "
        f"{len(df)} row(s)."
    )

    return df