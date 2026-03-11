import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

def transform_ad_insights(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform TikTok Ads ad insights
    ---
    Principles:
        1. Validate input
        2. Parse actions
        3. No need to resolve results
        4. Normalize date dimension
        5. Enforce numeric schema
    ---
    Returns:
        1. DataFrame:
            Enforced ad insights records
    """

    print(
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of TikTok Ads ad insights..."
    )

    if df.empty:
        
        print(
            "⚠️ [TRANSFORM] Empty TikToks Ads ad insights then transformation will be suspended."
        )
        
        return df

    required_cols = {
        "stat_time_day"
    }

    missing = required_cols - set(df.columns)
    
    if missing:
    
        raise ValueError(
            "❌ [TRANSFORM] Failed to transform TikTok Ads ad insights due to missing columns "
            f"{missing} then transformation will be suspended."
        )
    
    # Normalize numeric metrics
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

    # Normalize date dimension
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
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of TikTok Ads ad insights."
    )

    return df