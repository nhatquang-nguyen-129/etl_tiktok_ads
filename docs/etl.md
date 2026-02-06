# ETL Architecture and Workflow for Facebook Ads

## Purpose

- Use **dbt** to build Facebook analytics-ready **materialized tables** in **Google BigQuery**
- Used **dbt** only for **SQL transformations** and all ELT processes are handled upstream
- Join Facebook Ads campaign insights fact tables with campaign metadata dim table
- Join Facebook Ads ad insights fact tables with campaign metadata/adset metadata/ad metadata/ad creative dim tables
- Define final analytical grain and manage model dependencies using `ref()`

---

## Extract

### Activate Python venv

- Create Python virtual environment if `venv\` folder not exists
```bash
python -m venv venv
```

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
venv/scripts/activate
```

---

### Install dbt adapter for Google BigQuery

- Install dbt adapter for Google BigQuery using the terminal
```bash
pip install dbt-core dbt-bigquery
```

- Verify installation and check installed dbt version
```bash
dbt --version
```

---

## Structure

### Models folder

- `models` is root folder for all dbt models and all logical separation by transformation stage

- `models/stg` is the staging layer providing a clean abstraction over ETL output tables and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['stg', 'facebook', 'campaign']
) }}
```

- `models/int` is the intermediate layer with the responsibilty to combine staging models then join with dimensions and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['int', 'facebook', 'campaign']
) }}
```

- `models/mart` is the final materialization layer and materialized as `table` with example:
```bash
{{ config(
    materialized='table',
    tags=['mart', 'facebook', 'campaign']
) }}
```

---

### Config file

- `dbt_project.yml` is a required file for all dbt project which contains project operation instructions

- `profiles.yml` is a required file which contains the connection details for the data warehouse

---

## Deployment

### Manual Deployment

- Complie only with no execution
```bash
dbt compile
```

- Run all models
```bash
dbt build
```

- Run only campaign insights
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

dbt build `
  --project-dir dbt `
  --profiles-dir dbt `
  --select tag:ad
```

- Run only ad insights
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

dbt build `
  --project-dir dbt `
  --profiles-dir dbt `
  --select tag:ad
```

### Deployment with DAGs

- Using Python `subprocess` to call dbt for each stream
```bash
dbt_facebook_ads(
    google_cloud_project=PROJECT,
    select="campaign",
)
```

## Main

### Resolve execution time window

- `main.py` will not start if any of the required environment variables are missing
- Resolve `MODE` into `start_date` and `end_date` then format them as `YYYY-MM-DD`
- Unsupported `MODE` values will immediately fail execution
- `last3days` will be resolved into `start_date` and `end_date` which is the last 3 days **except** yesterday
- `last7days` will be resolved into `start_date` and `end_date` which is the last 7 days **except** yesterday
- `thismonth` will be resolved into `start_date` and `end_date` from the first day of the current month to today

### Initialize Google Secret Manager client
- Create a single `SecretManagerServiceClient` to intialize global client
- Resolve Facebook Ads `account_id` from secret `{COMPANY}_secret_{DEPARTMENT}_facebook_account_id_{ACCOUNT}`
- Resolve Facebook Ads `access_token` from secret `{COMPANY}_secret_all_facebook_token_access_user`
- Secrets are always fetched from `versions/latest`

### Initialize Facebook Ads SDK
- Create a sing `FacebookAdsApi.init` to initialize global client
- Set `timeout` to `180` for long-running API calls

### Dispatch execution to DAG orchestrator
- Call `dags_facebook_ads` with `account_id`, `start_date` and `end_date`
- All retry logic, cooldowns, batching and ETL orchestration are handled **inside** DAGs
- CLI usage example for main entrypoint: 

```bash
$env:PROJECT="seer-digital-ads"
$env:COMPANY="kids"
$env:DEPARTMENT="marketing"
$env:ACCOUNT="main"
$env:MODE="last3days"

python main.py
```