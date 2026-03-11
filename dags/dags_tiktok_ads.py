import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from concurrent.futures import ThreadPoolExecutor

from dags._dags_campaign_insights import dags_campaign_insights
from dags._dags_ad_insights import dags_ad_insights

def dags_tiktok_ads(
    *,
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
    max_workers: int = 2,
):
    """
    DAG Orchestration for TikTok Ads
    ---
    Principles:
        1. Initialize parallel execution with worker pool
        2. Submit campaign-level and ad-level tasks concurrently
        3. Monitor task completion using asynchronous future handling
        4. Capture execution status and surface task-level failures
        5. Finalize DAG execution with total runtime reporting
    ---
    Returns:
        1. None:
    """

    print(
        "🔄 [DAGS] Trigger to update TikTok Ads using ThreadPoolExecutor for account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date} with max_workers "
        f"{max_workers}..."
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                fn,
                access_token=access_token,
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
            )
            for fn in [
                dags_campaign_insights,
                dags_ad_insights,
            ]
        ]
        
        for future in futures:
            future.result()