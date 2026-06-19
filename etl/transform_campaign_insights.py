import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_campaign_insights(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform TikTok Ads campaign insights
    ---
    Principles:
        1. Validate input
        2. Parse actions
        3. Resolve results
        4. Normalize date dimension
        5. Enforce numeric schema
    ---
    Returns:
        1. DataFrame:
            Enforced campaign insights records
    """

    # Validate input
    print(
        "🔄 [TRANSFORM] Validating column(s) for "
        f"{len(df)} row(s) of TikTok Ads campaign insights..."
    )

    if df.empty:

        raise ValueError(
            "❌ [TRANSFORM] Failed to validate column(s) for TikTok Ads campaign insights due to empty input DataFrame."
        )

    required_cols = {
        "stat_time_day"
    }

    actual_cols = {
        str(col).strip()
        for col in df.columns
    }

    missing_cols = required_cols - actual_cols

    extra_cols = actual_cols - required_cols

    print(
        "✅ [TRANSFORM] Successfully validated DataFrame for TikTok Ads campaign insights with "
        f"{df.shape} shape with total column(s) "
        f"{len(actual_cols)}/{len(required_cols)} total column including "
        f"{len(missing_cols)} missing column(s) and "
        f"{len(extra_cols)} extra column(s)."
    )

    if missing_cols:

        raise ValueError(
            "❌ [TRANSFORM] Failed to transform validated DataFrame for TikTok Ads campaign insights due to missing required column(s) "
            f"{sorted(missing_cols)}"
        )

    # Parse numeric metrics
    for col in [
        "impressions", 
        "clicks", 
        "spend",
        "result",
        "engaged_view_15s",
        "onsite_shopping",
        "offline_shopping_events",
        "complete_payment",
        "onsite_total_purchase",
        "purchase",
        "messaging_total_conversation_tiktok_direct_message",
    ]:
        
        if col in df.columns:
        
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Parse date dimension
    if "stat_time_day" in df.columns:
        
        dt = pd.to_datetime(df["stat_time_day"], errors="coerce", utc=True)
        
        df["date"] = dt.dt.floor("D")
        
        df["year"] = dt.dt.year
        
        df["month"] = dt.dt.strftime("%Y-%m")

    # Drop raw columns
    df = df.drop(
        columns=["stat_time_day"],
        errors="ignore"
    )

    print(
        "✅ [TRANSFORM] Successfully transformed TikTok Ads ad insights with "
        f"{len(df)} row(s)."
    )

    return df