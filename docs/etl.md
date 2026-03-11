# ETL for TikTok Ads

## Purpose

- **Extract** TikTok Ads data from `TikTok Ads Open API V1.3`

- **Transform** raw records into `normalized analytical schema`

- **Load** transformed data into `Google BigQuery` using idempotent `UPSERT` strategy

---

## Extract

- The extractor initializes a TikTok Ads API client using `access_token`

- The extractor retrieves advertiser metadata from endpoint `GET/open_api/v1.3/advertiser/info/`

- The extractor retrieves campaign metadata from endpoint `GET/open_api/v1.3/campaign/get/`

- The extractor retrieves ad metadata from endpoint `GET/open_api/v1.3/ad/get/`

- The extractor retrieves performance insights from endpoint `GET/open_api/v1.3/report/integrated/get/`

- The extractor enforces reporting level using request parameter `data_level = advertiser | campaign | adgroup | ad`

- The extractor enforces date filtering using parameters `start_date` and `end_date`

- The extractor applies pagination using page and `page_size` parameters until the API returns `has_more = false`

- The extractor converts JSON API responses into flattened `List[dict]` records

- The extractor converts extracted records into `pandas.DataFrame`

- The extractor propagates structured error metadata using `error.retryable` flag to support orchestration-level retry logic

---

## Transform

- The transformer normalizes `advertiser_id`, `campaign_id`, `adgroup_id` and `ad_id` to STRING type

- The transformer enforces numeric schema for `impressions` and `clicks` as `INT64`

- The transformer converts cost into spend by casting to `INT64`

- The transformer enforces floating-point schema for `conversion` or `result` metrics

- The transformer normalizes `stat_time_day` into `UTC` timestamp and floors to daily granularity

- The transformer derives `year` dimension from normalized date

- The transformer derives `month` dimension using `YYYY-MM` format from normalized date

- The transformer enriches a constant platform column with value `TikTok`

- The transformer parses `campaign_name` using underscore `_` delimiter to derive structured dimensions

- The transformer parses `adgroup_name` using underscore `_` delimiter to derive structured dimensions

- The transformer fills missing parsed values with `unknown` to preserve schema consistency

---

## Load

- The loader uses `mode="upsert"` to support idempotent loading and deduplication

- The loader uses primary key(s) defined in `keys=[...]` to overwrite existing matching records

- The loader delegates execution to `internalGoogleBigqueryLoader` for standardized BigQuery operations

- The loader uses `keys=["date"]` to deduplicate campaign and ad insights records at daily granularity

- The loader applies table partitioning on `partition={"field": "date"}` for campaign and ad insights

- The loader applies table clustering on `cluster=["campaign_id"]` for campaign and insights

- The loader uses composite primary keys `keys=["advertiser_id", "campaign_id"]` for campaign metadata

- The loader uses composite primary keys `keys=["advertiser_id", "ad_id"]` for ad metadata

- The loader applies table clustering on `cluster=["campaign_id"]` for campaign metadata

- The loader applies table clustering on `cluster=["ad_id"]` for ad metadata

- The loader does not apply table partitioning for campaign or ad metadata