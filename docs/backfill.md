# Backfill for TikTok Ads

## Purpose

- Execute TikTok Ads DAGs for **historical data** outside of default runtime schedule

- Allow **manual override** of time range instead of relying on `main.py` predefined `MODE` mappings

- Require standard environment variables `COMPANY`, `PROJECT`, `DEPARTMENT`, `ACCOUNT` to be provided

- Primarily designed to run in **local environment** for controlled historical reprocessing

- Accept `start_date` and `end_date` via argparse for flexible time-based backfill execution

---

## Execution

- Ensure Google Cloud Platform's Application Default Credentials already configured

- Run Backfill for TikTok Ads campaign insights with specific date range using CLI
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

python -m backfill.backfill_campaign_insights --start_date=2026-01-05 --end_date=2026-01-05
```

- Run Backfill for TikTok Ads ad insights with specific date range using CLI
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

python -m backfill.backfill_ad_insights --start_date=2026-01-05 --end_date=2026-01-05
```

- Run Backfill for TikTok Ads with specific date range using CLI
```bash
$env:PROJECT="your-gcp-project"
$env:COMPANY="your-company-in-short"
$env:DEPARTMENT="your-department"
$env:ACCOUNT="your-account"

python -m backfill.backfill_tiktok_ads --start_date=2026-01-05 --end_date=2026-01-05
```