# Test — Backfill for Main Entrypoin

- Manually fetch historical Google Ads data outside predefined `MODE` window
- Read required environment variables `COMPANY`, `PROJECT`, `DEPARTMENT`, `ACCOUNT`
- Accept `start_date` and `end_date` from CLI
- CLI usage example: 

```bash
export COMPANY=my_company
export PROJECT=my_gcp_project
export DEPARTMENT=marketing
export ACCOUNT=search

python backfill.py --start-date 2025-12-30 --end-date 2026-01-01`