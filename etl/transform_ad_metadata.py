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

    # Validate input
    print(
        "🔄 [TRANSFORM] Validating column(s) for "
        f"{len(df)} row(s) of TikTok Ads ad metadata..."
    )

    if df.empty:

        raise ValueError(
            "❌ [TRANSFORM] Failed to validate column(s) for TikTok Ads ad metadata due to empty input DataFrame."
        )

    required_cols = {
        "advertiser_id",
        "adgroup_id",
        "adgroup_name",
        "ad_id",
        "ad_name",
        }
    
    actual_cols = {
        str(col).strip()
        for col in df.columns
    }

    missing_cols = required_cols - actual_cols

    extra_cols = actual_cols - required_cols

    print(
        "✅ [TRANSFORM] Successfully validated DataFrame for TikTok Ads ad metadata with "
        f"{df.shape} shape with total column(s) "
        f"{len(actual_cols)}/{len(required_cols)} total column including "
        f"{len(missing_cols)} missing column(s) and "
        f"{len(extra_cols)} extra column(s)."
    )

    if missing_cols:

        raise ValueError(
            "❌ [TRANSFORM] Failed to transform validated DataFrame for TikTok Ads ad metadata due to missing required column(s) "
            f"{sorted(missing_cols)}"
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