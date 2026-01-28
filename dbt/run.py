import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import logging
import os
import subprocess

def dbt_tiktok_ads(
    *,
    google_cloud_project: str,
    select: str = "tag:mart",
):
    """
    Run dbt for TikTok Ads
    ---------
    Workflow:
        1. Initialize dbt execution environment
        2. Trigger dbt build command for dbt models
        3. Capture dbt execution logs with stdout and stderr
    ---------
    Returns:
        1. subprocess.CompletedProcess:
            Contains dbt execution result including stdout, stderr, and return code
    """

    msg = (
        "🔁 [DBT] Running dbt build for TikTok Ads with selector "
        f"{select} to Google Cloud Project "
        f"{google_cloud_project}..."
    )
    print(msg)
    logging.info(msg)

    cmd = [
        "dbt",
        "build",
        "--project-dir", "dbt",
        "--profiles-dir", "dbt",
        "--select", select,
    ]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            env={**os.environ},
            text=True,
        )

        print(result.stdout)
        logging.info(result.stdout)

        msg = (
            "✅ [DBT] Successfully completed dbt build for TikTok Ads with selector "
            f"{select} to Google Cloud Project "
            f"{google_cloud_project}."
        )
        print(msg)
        logging.info(msg)

    except subprocess.CalledProcessError as e:
        print(e.stdout)
        logging.error(e.stdout)
        print(e.stderr)
        logging.error(e.stderr)
        raise RuntimeError(
            "❌ [DBT] Failed to complete dbt build for TikTok Ads with selector " 
            f"{select} to Google Cloud Project "
            f"{google_cloud_project} due to {e}."
        )