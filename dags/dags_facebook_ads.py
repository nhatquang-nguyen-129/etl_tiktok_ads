import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time

from dags._dags_campaign_insights import dags_campaign_insights
from dags._dags_ad_insights import dags_ad_insights


def dags_facebook_ads(
    *,
    account_id: str,
    start_date: str,
    end_date: str,
    max_workers: int = 2,
):

    msg = (
        "🔁 [DAGS] Triggering to update Facebook Ads for account_id "
        f"{account_id} from "
        f"{start_date} to "
        f"{end_date} using ThreadPoolExecutor with "
        f"{max_workers} max_workers..."
    )
    print(msg)
    logging.info(msg)

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    exceptions = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}

        for name, task in tasks.items():
            msg = (
                "🔍 [DAGS] Submitting Facebook Ads ThreadPoolExecutor task "
                f"{name}..."
            )
            print(msg)
            logging.info(msg)

            future = executor.submit(
                task,
                account_id=account_id,
                start_date=start_date,
                end_date=end_date,
            )
            future_map[future] = name

        msg = (
            "🔍 [DAGS] Waiting for Facebook Ads ThreadPoolExecutor "
            f"{len(future_map)} task(s) to complete..."
        )
        print(msg)
        logging.info(msg)

        for future in as_completed(future_map):
            task_name = future_map[future]
            
            msg = (
                "🔍 [DAGS] Getting result from Facebook Ads ThreadPoolExecutor task "
                f"{task_name}..."
            )
            print(msg)
            logging.info(msg)

            try:
                future.result()
                msg = (
                    "✅ [DAGS] Successfully complete Facebook Ads ThreadPoolExecutor task "
                    f"{task_name}..."
                )
                print(msg)
                logging.info(msg)

            except Exception as e:
                msg = (
                    "❌ [DAGS] Failed to complete Facebook Ads ThreadPoolExecutor task "
                    f"{task_name} due to {e}."
                )
                print(msg)
                logging.error(msg)
                exceptions.append((task_name, e))

    elapsed = round(time.time() - start_time, 2)

    if exceptions:
        failed_tasks = ", ".join(name for name, _ in exceptions)
        raise RuntimeError(
            "❌ [DAGS] Failed to update Facebook Ads using ThreadPoolExecutor with"
            f"{failed_tasks} failed task(s) in "
            f"{elapsed}s elapsed time."
        )

    msg = (
        "✅ [DAGS] Successfully updated Facebook Ads using ThreadPoolExecutr in "
        f"{elapsed}s elapsed time."
    )
    print(msg)
    logging.info(msg)