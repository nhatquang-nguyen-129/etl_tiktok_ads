# Internal Google BigQuery Loader Plugin for TikTok Ads

## Purpose

- Provide a standardized internal abstraction layer for writing **pandas.DataFrame** into Google BigQuery

- Stateful loader class responsible designed to be **reusable** across DAGs 

- Support **INSERT** and **UPSERT** conflict handling strategies

- Dynamically **infer Google BigQuery schema** from Python DataFrame

- Support optional **partitioning** and **clustering** configuration

---

## Architecture

### Initialize Google BigQuery Client

- All table references must follow strict format `project.dataset.table`

- The loader extracts project from direction to initialize `bigquery.Client(project=project)`

- Initialization may fail due to invalid direction format or missing permissions

---

### Validate dataset existence

- The loader checks dataset existence with `_check_dataset_exist()`

- The loader calls client.get_dataset() then returns `True` if exists 

- If a `NotFound` exception is raised, it returns `False` then calls `_create_new_dataset()`

---

### Create dataset if missing

- If the dataset does not exist, the loader creates dataset 

- The loader uses default location `asia-southeast1`

- The loader uses `exists_ok=True` while creating new dataset for idempotency

---

### Validate table existence

- The loader checks table existence with `_check_table_exist()`

- The loader calls client.get_table() then returns `True` if exists 

- If a `NotFound` exception is raised, it returns `False` then calls `_create_new_table()`

---

### Create table if missing

- The loader uutomatically maps pandas dtypes to Google BigQuery types with `_infer_table_schema()`

- The loader automatically maps pandas dtype `int` to Google BigQuery type `INT64`

- The loader automatically maps pandas dtype `float` to Google BigQuery type `FLOAT64`

- The loader automatically maps pandas dtype `bool` to Google BigQuery type `BOOL`

- The loader automatically maps pandas dtype `datetime` to Google BigQuery type `TIMESTAMP`

- The loader automatically maps pandas dtype `others` to Google BigQuery type `STRING`

- The loader fully supports time partitioning with `PARTITION BY DAY(date)` and clusterting

- After dynamically inferring the schema, the loader create new table with `_create_new_table()` if the table does not exist

---

### Handle conflict logic

- The loader controls conflict behavior using the mode parameter `INSERT` or `UPSERT`

- With `INSERT` mode, the loader skips conflict detection and it does not delete any existing records

- With `INSERT` mode, the loader appends incoming data directly to the table and may create duplicates

- With `UPSERT` mode, the loader requires keys to be provided and these keys must exist in the DataFrame

- With `UPSERT` mode, the loader validates that all keys are present before execution 

- With `UPSERT` mode, the loader removes existing records in Google BigQuery table that match the provided keys

- With `UPSERT` mode, the loader executes a parameterized `DELETE` query using `UNNEST(@values)` if only one key is provided

- With `UPSERT` mode, the loader executes a `DELETE` statement using an `EXISTS` join condition with a temporary table

---

### Write DataFrame to Google BigQuery

- The loader calls `load_table_from_dataframe()` with `WRITE_APPEND` composition to upload into Google BigQuery

- The loader raises a runtime error immediately if the write operation fails