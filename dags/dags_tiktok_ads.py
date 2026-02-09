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
        f"{advertiser_id} from {start_date} to {end_date} "
        f"with {max_workers} max_workers..."
    )

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for name, fn in tasks.items():

            buffer = io.StringIO()
            buffers[name] = buffer

            def _run_task(fn=fn, buffer=buffer):
                original_stdout = sys.stdout
                original_stderr = sys.stderr
                try:
                    sys.stdout = buffer
                    sys.stderr = buffer
                    return fn(
                        access_token=access_token,
                        advertiser_id=advertiser_id,
                        start_date=start_date,
                        end_date=end_date,
                    )
                finally:
                    sys.stdout = original_stdout
                    sys.stderr = original_stderr

            futures[executor.submit(_run_task)] = name

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

    print(
        "✅ [DAGS] Successfully triggered to update TikTok Ads insights using "
        f"ThreadPoolExecutor in {round(time.time() - start_time, 2)}s."
    )