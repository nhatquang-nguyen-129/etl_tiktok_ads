# Backfill for TikTok Ads Main Entrypoint

- Manually fetch historical Google Ads data outside predefined `MODE` window
- Read required environment variables `COMPANY`, `PROJECT`, `DEPARTMENT`, `ACCOUNT`
- Accept `start_date` and `end_date` from CLI
- CLI usage example for campaign insights backfill: 

```bash
$env:PROJECT ="seer-digital-ads"; 
$env:COMPANY="kids"; 
$env:PLATFORM="tiktok"; 
$env:DEPARTMENT="marketing"; 
$env:ACCOUNT="main"; 
python -m backfill.backfill_campaign_insights --start_date=2026-01-23 --end_date=2026-01-23
```

- CLI usage example for ad insights backfill: 

```bash
$env:PROJECT ="seer-digital-ads"; 
$env:COMPANY="kids"; 
$env:PLATFORM="tiktok"; 
$env:DEPARTMENT="marketing"; 
$env:ACCOUNT="main"; 
python -m backfill.backfill_ad_insights --start_date=2026-01-23 --end_date=2026-01-23
```

- CLI usage example both threads using ThreadPoolExecutor: 

```bash
$env:PROJECT ="seer-digital-ads"; 
$env:COMPANY="kids"; 
$env:PLATFORM="tiktok"; 
$env:DEPARTMENT="marketing"; 
$env:ACCOUNT="main"; 
python -m backfill.backfill_tiktok_ads --start_date=2026-01-23 --end_date=2026-01-23
```