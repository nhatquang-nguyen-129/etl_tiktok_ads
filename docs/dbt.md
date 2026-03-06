# DAGs for TikTok Ads

## Purpose

- Execute TikTok Ads DAGs using **predefined runtime configurations**

- Resolve **execution time window** indirectly via `main.py` instead of `--start_date` and `--end_date` via argparse

- Designed primarily for **production deployment** on Cloud Run

- Automatically load **required secrets** from Google Secret Manager

- Dispatch execution to the **DAG orchestrator** without exposing manual entrypoints

---

## Execution

### Runtime Contract

 - The following environment variables `COMPANY`, `PROJECT`, `DEPARTMENT` and `ACCOUNT` must be provided

- The DAGS execution time logic is controlled externally by `main.py` with predefined runtime modes

- The resolved execution context is then passed to `dags_tiktok_ads`

---

### Secret Management

- `main.py` initializes Google Secret Manager client to resolves required secrets

- `main.py` retrieves `secret_ac_id` from `{COMPANY}_secret_{DEPARTMENT}_facebook_account_id_{ACCOUNT}`

- `main.py` retrieves `secret_account_name` from `projects/{PROJECT}/secrets/{secret_account_id}/versions/latest`

- `main.py` retrieves `secret_token_id` from `{COMPANY}_secret_all_facebook_token_access_user`

- `main.py` retrieves `secret_token_name ` from `projects/{PROJECT}/secrets/{secret_token_id}/versions/latest`

---

### Production Deployment

- This runtime is intended to run inside `Cloud Run` and triggered by `Cloud Scheduler`

- This runtime is technically possible for local execution but not recommended

- For manual historical reprocessing, use the `Backfill` module instead

- Run DAGs for specific `MODE` using CLI
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"
$env:MODE="your-time-window"

python main.py
```