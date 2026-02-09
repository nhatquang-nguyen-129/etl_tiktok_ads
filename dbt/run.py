import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import os
import subprocess

def dbt_tiktok_ads(
    *,
    google_cloud_project: str,
    select: str
):
    """
    Run dbt for TikTok Ads
    ---------
    Workflow:
        1. Initialize dbt execution environment
        2. Initialize Python subprocess to execute CLI
        3. Execute dbt build command with environment variables
        4. Execute dbt build command for dbt models stg/int/mart
        3. Capture dbt execution status with stdout and stderr
    ---------
    Returns:
        None
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        print(
            "✅ [DBT] Successfully executed dbt build for TikTok Ads "
            f"{select} insights to Google Cloud Project "
            f"{google_cloud_project}."
        )

        # Buffered dbt global logging
        return {
            "status": "SUCCESS",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    except subprocess.CalledProcessError as e:
        return {
            "status": "FAILED",
            "stdout": e.stdout,
            "stderr": e.stderr,
            "error": str(e),
        }