import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for name, fn in tasks.items():
            start_ts = time.time()
            future = executor.submit(
                fn,
                access_token=access_token,
                advertiser_id=advertiser_id,
                start_date=start_date,
                end_date=end_date,
            )
            futures[future] = (name, start_ts)

        for future in as_completed(futures):
            name, start_ts = futures[future]
            duration = round(time.time() - start_ts, 2)

            try:
                future.result()
                results[name] = {
                    "status": "SUCCESS",
                    "duration": duration,
                    "detail": "",
                }
            except Exception as e:
                results[name] = {
                    "status": "FAILED",
                    "duration": duration,
                    "detail": str(e),
                }

    print("\n📊 [DAGS] TIKTOK EXECUTION SUMMARY")
    print("=" * 120)
    print(f"{'TASK':<30} | {'STATUS':<10} | {'DURATION(s)':<12} | DETAIL")
    print("-" * 120)

    for name, r in results.items():
        print(
            f"{name:<30} | "
            f"{r['status']:<10} | "
            f"{r['duration']:<12} | "
            f"{r['detail']}"
        )
