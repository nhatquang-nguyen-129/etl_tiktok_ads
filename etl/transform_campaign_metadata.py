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
    ---------
    Workflow:
        1. Validate input
        2. Validate missing columns
        3. Assign enriched columns
    ---------
    Returns:
        1. DataFrame:
            Enforced campaigns metadata records
    """

    print(
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of TikTok Ads campaign metadata..."
    )

    if df.empty:
        print("⚠️ [TRANSFORM] Empty campaign metadata then transformation will be suspended.")
        return df

    required_cols = {
        "advertiser_id",
        "campaign_id",
        "campaign_name"
        }
    
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError (
            "❌ [TRANSFORM] Failed to transform TikTok Ads campaign metadata due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()
    df["platform"] = "TikTok"
    df = df.assign(
        objective=df["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        region=df["campaign_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        budget_group_1=df["campaign_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        budget_group_2=df["campaign_name"].fillna("").str.split("_").str[3].fillna("unknown"),      
        category_level_1=df["campaign_name"].fillna("").str.split("_").str[4].fillna("unknown"),
        personnel=df["campaign_name"].fillna("").str.split("_").str[5].fillna("unknown"),
        track_group=df["campaign_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        pillar_group=df["campaign_name"].fillna("").str.split("_").str[8].fillna("unknown"),
        content_group=df["campaign_name"].fillna("").str.split("_").str[9].fillna("unknown"),
    )

    print(
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of TikTok Ads campaign metadata."
    )

    return df