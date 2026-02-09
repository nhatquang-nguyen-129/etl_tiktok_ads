import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from dags._dags_campaign_insights import dags_campaign_insights
from dags._dags_ad_insights import dags_ad_insights

def dags_tiktok_ads(
    *,
    access_token: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    max_workers: int = 2,
):

    start_time = time.time()
    futures = {}

    print(
        "🔄 [DAGS] Triggering to update TikTok Ads insights for "
        f"{advertiser_id} from "
        f"{start_date} to "
        f"{end_date} with " 
        f"{max_workers} max_workers..."
    )

    # Submit tasks
    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for name, fn in tasks.items():
            print(
                "🔄 [DAGS] Triggering to execute "
                f"{name} task with ThreadPoolExecutor..."
            )
            
            future = executor.submit(
                fn,
                access_token=access_token,
                advertiser_id=advertiser_id,
                start_date=start_date,
                end_date=end_date,
            )
            futures[future] = name

        completed = set()

        for future in as_completed(futures):
            name = futures[future]
            completed.add(name)

            try:
                future.result()
                print(f"📊 [DAGS] TASK EXECUTION SUMMARY FOR {name}")
                print("=" * 120)
                print("STATUS : SUCCESS")
                print("-" * 120)
            except Exception as e:
                print(f"📊 [DAGS] TASK EXECUTION SUMMARY FOR {name}")
                print("=" * 120)
                print("STATUS : FAILED")
                print("-" * 120)
                print(str(e))          

            remaining_tasks = [n for n in tasks if n not in completed]
            if remaining_tasks:
                print("\n📊 [DAGS] REMAINNING TASK LIST:")
                for remaining_task in remaining_tasks:
                    print(
                        f"{remaining_task:<25} : Executing")
                print()

    total_elapsed = round(time.time() - start_time, 2)
    print(
        "✅ [DAGS] Successfully triggered to update TikTok Ads insights using ThreadPoolExecutor in "
        f"{total_elapsed}s."
        )