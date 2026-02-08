# Data Build Tool for TikTok Ads SQL Materialization

## Purpose

- Use **dbt** to build TikTok analytics-ready **materialized tables** in **Google BigQuery**
- Used **dbt** only for **SQL transformations** and all ELT processes are handled upstream
- Join TikTok Ads campaign insights fact tables with campaign metadata dim table
- Join TikTok Ads ad insights fact tables with campaign metadata/adset metadata/ad metadata/ad creative dim tables
- Define final analytical grain and manage model dependencies using `ref()`

---

## Install

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
    tags=['stg', 'tiktok', 'campaign']
) }}
```

- `models/int` is the intermediate layer with the responsibilty to combine staging models then join with dimensions and materialized as `ephemeral` with example:
```bash
{{ config(
    materialized='ephemeral',
    tags=['int', 'tiktok', 'campaign']
) }}
```

- `models/mart` is the final materialization layer and materialized as `table` with example:
```bash
{{ config(
    materialized='table',
    tags=['mart', 'tiktok', 'campaign']
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
$env:PROJECT="seer-digital-ads"
$env:COMPANY="kids"
$env:DEPARTMENT="marketing"
$env:ACCOUNT="main"

dbt build `
  --project-dir dbt `
  --profiles-dir dbt `
  --select tag:campaign
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
dbt_tiktok_ads(
    google_cloud_project=PROJECT,
    select="campaign",
)
```