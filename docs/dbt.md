# dbt — Google Ads Analytics Mart

## Purpose

- Use **dbt** to build analytics-ready **mart tables** in **BigQuery**
- Join **fact Google Ads campaign insights** with:
  - campaign metadata
  - campaign creative
- dbt is used **only for SQL transformations**
- All extraction, enrichment, and normalization are handled **upstream in ETL**

---

## What dbt Does

- Join fact and dimension tables
- Define final analytical grain
- Build mart tables for BI/analytics
- Manage model dependencies using `ref()`

---

## What dbt Does NOT Do

- Extract data from APIs
- Enrich or clean raw data
- Perform complex business logic
- Orchestrate pipelines or schedules
- Replace ETL validation logic

---

## Data Flow

- Load prepared tables via ETL
- Use dbt to join and reshape data
- Output analytics-ready marts