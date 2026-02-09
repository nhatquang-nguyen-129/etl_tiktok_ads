# Data Build Tool for TikTok Ads SQL Materialization

## Purpose

- Use **dbt** to build TikTok analytics-ready **materialized tables** in **Google BigQuery**

- Used **dbt** only for **SQL transformations** and all ELT processes are handled upstream

- Join TikTok Ads campaign insights fact tables with campaign metadata dim table

- Join TikTok Ads ad insights fact tables with campaign metadata/adset metadata/ad metadata/ad creative dim tables

- Define final analytical grain and manage model dependencies using `ref()`

---

## Install

### Switch to Python 3.13 Interpreter
- Explicitly choose the correct Python interpreter if multiple versions was installed

- Create a Python virtual environment using Python 3.13 interpreter when run from the root folder
```bash
& "C:\Users\ADMIN\AppData\Local\Programs\Python\Python313\python.exe" -m venv venv
```

- Check available Python interpreter if there is any uncertainty by press `Ctrl + Shift + P` then select `Python: Select Interpreter`

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
venv/scripts/activate
```

- Verify Python virtual environment and check Python Interpreter version
```bash
python --version
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
$env:PROJECT="seer-digital-ads"
$env:COMPANY="kids"
$env:DEPARTMENT="marketing"
$env:ACCOUNT="main"

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