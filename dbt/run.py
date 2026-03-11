import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import subprocess

def dbt_tiktok_ads(
    *,
    google_cloud_project: str,
    select: str
):
    """
    DBT Execution for TikTok Ads
    ---
    Principles:
        1. Initialize dbt CLI execution environment
        2. Construct dbt build command with model selection
        3. Execute dbt build within project directory context
        4. Capture subprocess execution status and surface failures
        5. Finalize execution with success confirmation
    ---
    Returns:
        1. None:
    """

    cmd = [
        "dbt",
        "build",
        "--profiles-dir", ".",
        "--select", select,
    ]

    print(
        "🔄 [DBT] Executing dbt build for TikTok Ads "
        f"{select} insights to Google Cloud Project "
        f"{google_cloud_project}..."
    )

    try:
        result = subprocess.run(
            cmd,
            cwd="dbt",
            env=os.environ,
            check=True,
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        print(
            "✅ [DBT] Successfully executed dbt build for TikTok Ads "
            f"{select} insights to Google Cloud Project "
            f"{google_cloud_project}."
        )

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            "❌ [DBT] Failed to execute dbt build for TikTok Ads "
            f"{select} insights to Google Cloud Project "
            f"{google_cloud_project} due to "
            f"{e}."
        ) from e