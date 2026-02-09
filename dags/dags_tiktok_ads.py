import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout, redirect_stderr
import io
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

    start_time = time.time()
    buffers = {}
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
                "🔄 [DAGS] Submitting task "
                f"{name} to ThreadPoolExecutor..."
            )

            buffer = io.StringIO()
            buffers[name] = buffer

            future = executor.submit(
                lambda fn=fn, buffer=buffer: (
                    redirect_stdout(buffer),
                    redirect_stderr(buffer),
                    fn(
                        access_token=access_token,
                        advertiser_id=advertiser_id,
                        start_date=start_date,
                        end_date=end_date,
                    ),
                )[-1]
            )

            futures[future] = name

        completed = set()

        for future in as_completed(futures):
            name = futures[future]
            completed.add(name)

            print(f"\n📊 [DAGS] TASK EXECUTION SUMMARY :: {name}")
            print("=" * 120)

            try:
                future.result()
                print("STATUS : SUCCESS")
            except Exception as e:
                print("STATUS : FAILED")
                print("-" * 120)
                print(str(e))

            print("-" * 120)
            print("EXECUTION LOG:")
            print(buffers[name].getvalue().rstrip())
            print("=" * 120)

            remaining_tasks = [n for n in tasks if n not in completed]
            if remaining_tasks:
                print("\n📊 [DAGS] RUNTIME TASK STATE")
                print("=" * 120)
                for task in remaining_tasks:
                    print(f"{task:<30} | STATUS = RUNNING")
                print("=" * 120)

    total_elapsed = round(time.time() - start_time, 2)
    print(
        "✅ [DAGS] Successfully triggered to update TikTok Ads insights using "
        f"ThreadPoolExecutor in {total_elapsed}s."
    )