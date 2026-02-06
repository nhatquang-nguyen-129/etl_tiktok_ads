import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from dags._dags_campaign_insights import dags_campaign_insights
from dags._dags_ad_insights import dags_ad_insights


def dags_facebook_ads(
    *,
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
    max_workers: int = 2,
):
    print(
        f"🔄 [DAGS] Trigger Facebook Ads DAGs for {account_id} "
        f"from {start_date} → {end_date} | workers={max_workers}"
    )

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    start_time = time.time()
    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # submit ALL tasks
        for name, fn in tasks.items():
            print(f"▶️  [DAGS:{name}] RUNNING")
            future = executor.submit(
                fn,
                access_token=access_token,
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
            )
            futures[future] = name

        completed = set()

        for future in as_completed(futures):
            name = futures[future]
            completed.add(name)

            print("\n" + "=" * 120)
            print(f"[DAGS] TASK FINISHED: {name}")
            print("=" * 120)

            try:
                future.result()
                print(f"✅ [DAGS:{name}] COMPLETED")
            except Exception as e:
                print(f"❌ [DAGS:{name}] FAILED")
                print(str(e))

            print("=" * 120)

            remaining = [n for n in tasks if n not in completed]
            if remaining:
                print("\n⏳ Still running:")
                for r in remaining:
                    print(f"   ▶️  [DAGS:{r}] RUNNING")
                print()

    total_elapsed = round(time.time() - start_time, 2)
    print(f"🏁 [DAGS] Facebook Ads update finished in {total_elapsed}s")