import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_ad_metadata(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform TikTok Ads adset metadata
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
            Enforced ad metadata records
    """

    print(
        "🔄 [TRANSFORM] Transforming TikTok Ads ad metadata with "
        f"{len(df)} row(s)..."
    )

    if df.empty:
        
        print(
            "⚠️ [TRANSFORM] Empty ad metadata then transformation will be suspended."
        )
        
        return df

    required_cols = {
        "advertiser_id",
        "adgroup_id",
        "adgroup_name",
        "ad_id",
        "ad_name",
        }
    
    missing = required_cols - set(df.columns)
    
    if missing:
    
        raise ValueError (
            "❌ [TRANSFORM] Failed to transform TikTok Ads ad metadata due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()
    
    df = df.assign(
        location=lambda df: df["adgroup_name"].fillna("").str.split("|").str[0].fillna("unknown"),
        gender=lambda df: df["adgroup_name"].fillna("").str.split("|").str[1].fillna("unknown"),
        age=lambda df: df["adgroup_name"].fillna("").str.split("|").str[2].fillna("unknown"),
        audience=lambda df: df["adgroup_name"].fillna("").str.split("|").str[3].fillna("unknown"),
        format=lambda df: df["adgroup_name"].fillna("").str.split("|").str[4].fillna("unknown"),
        strategy=lambda df: df["adgroup_name"].fillna("").str.split("|").str[5].fillna("unknown"),
        angle=lambda df: df["adgroup_name"].fillna("").str.split("|").str[6].fillna("unknown"),
        content=lambda df: df["adgroup_name"].fillna("").str.split("|").str[7].fillna("unknown"),
        type=lambda df: df["adgroup_name"].fillna("").str.split("|").str[8].fillna("unknown")
    )  

    print(
        "✅ [TRANSFORM] Successfully transformed TikTok Ads ad metadata with "
        f"{len(df)} row(s)."
    )

    return df