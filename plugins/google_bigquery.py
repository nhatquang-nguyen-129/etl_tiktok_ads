import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd
import uuid

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

class internalGoogleBigqueryLoader:
    """
    Internal Google BigQuery Loader
    ---------
    Workflow:
        1. Initialize BigQuery client
        2. Check dataset existence
        3. Create dataset if not exist
        4. Check table existence
        5. Create table if not exist
        6. Apply INSERT/UPSERT DML
        7. Write data into table
    ---------
    Returns:
        None
    """

# 1.1. Initialize
    def __init__(self) -> None:
        self.client: bigquery.Client | None = None
        self.project: str | None = None

# 1.2. Loader
    def load(
        self,
        *,
        df: pd.DataFrame,
        direction: str,
        mode: str,
        keys: list[str] | None = None,
        partition: dict | None = None,
        cluster: list[str] | None = None,
    ) -> None:

        self._init_client(direction)

        project, dataset, _ = direction.split(".")

        if not self._check_dataset_exist(project, dataset):
            self._create_new_dataset(project, dataset)

        table_exists = self._check_table_exist(direction)

        if not table_exists:
            self._create_new_table(
                direction=direction,
                df=df,
                partition=partition,
                cluster=cluster,
            )

        self._handle_table_conflict(
            direction=direction,
            df=df,
            mode=mode,
            keys=keys,
            table_exists=table_exists,
        )

        self._write_table_data(
            df=df,
            direction=direction,
        )

# 1.3. Workflow

    # 1.3.1. Initialize client
    def _init_client(
            self, 
            direction: str
            ) -> None:
        
        if self.client:
            return

        try:
            print(
                "🔍 [PLUGIN] Initializing Google BigQuery client with direction "
                f"{direction}..."
                )

            parts = direction.split(".")
            if len(parts) != 3:
                raise ValueError(
                    "❌ [PLUGIN] Failed to initialize Google BigQuery client due to direction "
                    f"{direction} does not comply with project.dataset.table format."
                )

            project, _, _ = parts
            self.project = project
            self.client = bigquery.Client(project=project)
            
            print(
                "✅ [PLUGIN] Successfull initialized Google BigQuery client for project "
                f"{project}."
                )
        
        except Exception as e:
            raise RuntimeError(
                "❌ [PLUGIN] Failed to initialize Google BigQuery client for direction "
                f"{direction} due to "
                f"{str(e)}."
            )

    # 1.3.2. Check dataset existence
    def _check_dataset_exist(
            self, 
            project: str, 
            dataset: str
            ) -> bool:
        
        full_dataset_id = f"{project}.{dataset}"

        try:
            print(
                "🔍 [PLUGIN] Validating Google BigQuery dataset "
                f"{full_dataset_id} existence..."
            ) 

            self.client.get_dataset(full_dataset_id)

            print(
                "✅ [PLUGIN] Successfully validated Google BigQuery dataset "
                f"{full_dataset_id} existence."
            )

            return True

        except NotFound:
            print(
                "⚠️ [PLUGIN] Failed to find Google BigQuery dataset "
                f"{full_dataset_id} not found then dataset creation will be proceeding..."
            ) 
            
            return False

    # 1.3.3. Create dataset if not exist
    def _create_new_dataset(
            self, 
            project: str, 
            dataset: str,
            location: str = "asia-southeast1",
            ) -> None:
        
        full_dataset_id = f"{project}.{dataset}"
        
        try:
            print(
                "🔍 [PLUGIN] Creating Google BigQuery dataset "
                f"{full_dataset_id}..."
            ) 

            dataset_config = bigquery.Dataset(full_dataset_id)
            dataset_config.location = location
            self.client.create_dataset(dataset_config, exists_ok=True)

            print(
                "✅ [PLUGIN] Successfully created Google BigQuery dataset "
                f"{full_dataset_id}."
            ) 

        except Exception as e:
            raise RuntimeError(
                "❌ [PLUGIN] Failed to create Google BigQuery dataset "
                f"{full_dataset_id} due to "
                f"{str(e)}."
            )

    # 1.3.4. Infer DataFrame schema
    @staticmethod
    def _infer_table_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:
        schema = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                bq_type = "INT64"
            elif pd.api.types.is_float_dtype(dtype):
                bq_type = "FLOAT64"
            elif pd.api.types.is_bool_dtype(dtype):
                bq_type = "BOOL"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                bq_type = "TIMESTAMP"
            else:
                bq_type = "STRING"
            schema.append(bigquery.SchemaField(col, bq_type))
        return schema

    # 1.3.5. Check table existence
    def _check_table_exist(
            self, 
            direction: str
            ) -> bool:
        
        try:
            print(
                "🔍 [PLUGIN] Validating Google BigQuery table " 
                f"{direction} existence..."
            )
            
            self._init_client(direction)
            self.client.get_table(direction)
            
            print(
                "✅ [PLUGIN] Successfully validated Google BigQuery table " 
                f"{direction} existence."
            ) 

            return True

        except NotFound:
            print(
                "⚠️ [PLUGIN] Google BigQuery table "
                f"{direction} not found then table creation will be proceeding..."
            ) 
            
            return False

    # 1.3.6. Create new table
    def _create_new_table(
        self,
        *,
        direction: str,
        df: pd.DataFrame,
        partition: dict | None = None,
        cluster: list[str] | None = None,
    ) -> None:
        
        try:
            print(
                "🔍 [PLUGIN] Creating Google BigQuery table "
                f"{direction} with partition on "
                f"{partition} and cluster on "
                f"{cluster}..."
            )            

            table = bigquery.Table(
                direction,
                schema=self._infer_table_schema(df),
            )

            if partition:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition["field"],
                )

            if cluster:
                table.clustering_fields = cluster

            self.client.create_table(table)
            
            print(
                "✅ [PLUGIN] Successfully created Google BigQuery table "
                f"{direction}.")
        
        except Exception as e:
            raise RuntimeError(
                "❌ [PLUGIN] Failed to create Google BigQuery table "
                f"{direction} due to "
                f"{str(e)}."
            )

    # 1.3.7. Handle table conflict
    def _handle_table_conflict(
        self,
        *,
        direction: str,
        df: pd.DataFrame,
        mode: str,
        keys: list[str] | None,
        table_exists: bool | None = True,
    ) -> None:

        if mode == "insert":
            print(
                "⚠️ [PLUGIN] Applied INSERT upload mode for conflict handling then existing records deletion in Google BigQuery table "
                f"{direction} will be skipped."
            )
            return

        if mode == "upsert":
            if table_exists is False:
                print(
                    "⚠️ [PLUGIN] Applied UPSERT upload mode for conflict handling for new Google BigQuery table "
                    f"{direction} then existing records deletion will be skipped. "
                )
                return

            if not keys:
                raise ValueError(
                    "❌ [PLUGIN] Failed to apply UPSERT conflict handling due to deduplication keys is required for Google BigQuery table "
                    f"{direction}."
                )

            print(
                "🔄 [PLUGIN] Applying UPSERT for Google BigQuery table "
                f"{direction} with  "
                f"{keys} key(s)..."
            )

            missing = [k for k in keys if k not in df.columns]
            if missing:
                raise ValueError(
                    "❌ [PLUGIN] Failed to validate deduplication keys in DataFrame due to "
                    f"{missing} missing key(s)."
                )

            df_to_delete = df[keys].dropna().drop_duplicates()
            if df_to_delete.empty:
                print(
                    "⚠️ [PLUGIN] Applied UPSERT conflict handling but no keys found in DataFrame then existing records in Google BigQuery table "
                    f"{direction} will be skipped."
                )
                return

            # Single delete using parameterized query
            if len(keys) == 1:
                key = keys[0]
                series = df_to_delete[key]
                values = series.tolist()

                if not values:
                    return

                if pd.api.types.is_datetime64_any_dtype(series):
                    bq_type = "TIMESTAMP"
                elif pd.api.types.is_integer_dtype(series):
                    bq_type = "INT64"
                elif pd.api.types.is_float_dtype(series):
                    bq_type = "FLOAT64"
                elif pd.api.types.is_bool_dtype(series):
                    bq_type = "BOOL"
                else:
                    bq_type = "STRING"

                query_check_exist = f"""
                SELECT DISTINCT {key}
                FROM `{direction}`
                WHERE {key} IN UNNEST(@values)
                """

                job_check_exist = self.client.query(
                    query_check_exist,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ArrayQueryParameter(
                                "values",
                                bq_type,
                                values
                            )
                        ]
                    ),
                )

                existing_values = [row[key] for row in job_check_exist.result()]

                if not existing_values:
                    print(
                        "⚠️ [PLUGIN] Applied UPSERT conflict handling but no matching keys found in Google BigQuery table "
                        f"{direction} then existing records deletion via parameterized query will be skipped."
                    )
                    return

                print(
                    "🔍 [PLUGIN] Deleting existing row(s) in Google BigQuery table "
                    f"{direction}..."
                )
                
                query_delete_exist = f"""
                DELETE FROM `{direction}`
                WHERE {key} IN UNNEST(@values)
                """
                job_delete_exist = self.client.query(
                    query_delete_exist,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ArrayQueryParameter(
                                "values",
                                bq_type,
                                values
                            )
                        ]
                    ),
                )
                job_delete_exist.result()
                deleted_rows = job_delete_exist.num_dml_affected_rows or 0

                print(
                    "✅ [PLUGIN] Successfully deleted "
                    f"{deleted_rows} row(s) in Google BigQuery table"
                    f"{direction} using parameterized query with "
                    f"{key} key to delete."
                    
                )

                return

            # Batch delete using temporary table
            project, dataset, _ = direction.split(".")
            temp_table = (
                f"{project}.{dataset}._tmp_delete_keys_"
                f"{uuid.uuid4().hex[:8]}"
            )

            for k in keys:
                if df_to_delete[k].dtype != df[k].dtype:
                    raise TypeError(
                        "❌ [PLUGIN] Failed to delete existing records in Google BigQuery table "
                        f"{direction} due to dtype mismatch on key "
                        f"{k} with "
                        f"{df_to_delete[k].dtype} in temporary table versus "
                        f"{df[k].dtype} in direction."
                    )

            self.client.load_table_from_dataframe(
                df_to_delete,
                temp_table,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE"
                ),
            ).result()

            join_condition = " AND ".join(
                [f"main.{k} = temp.{k}" for k in keys]
            )

            query_check_exist = f"""
            SELECT COUNT(1) AS cnt
            FROM `{direction}` AS main
            WHERE EXISTS (
                SELECT 1
                FROM `{temp_table}` AS temp
                WHERE {join_condition}
            )
            """

            job_check_exist = self.client.query(query_check_exist)
            existing_rows = list(job_check_exist.result())
            existing_count = existing_rows[0]["cnt"] if existing_rows else 0

            if existing_count == 0:
                print(
                    "⚠️ [PLUGIN] Applied UPSERT conflict handling but no matching composite keys found in Google BigQuery table "
                    f"{direction} then existing rows deletion via temporary table will be skipped."
                )
                return
            
            print(
                "🔍 [PLUGIN] Deleting "
                f"{existing_count} existing row(s) in Google BigQuery table..."
                f"{direction}..."
            )          

            try:
                job_delete_exist = self.client.query(
                    f"""
                    DELETE FROM `{direction}` AS main
                    WHERE EXISTS (
                        SELECT 1
                        FROM `{temp_table}` AS temp
                        WHERE {join_condition}
                    )
                    """
                )
                deleted_rows = job_delete_exist.result().num_dml_affected_rows or 0

                print(
                    "✅ [PLUGIN] Successfully deleted "
                    f"{deleted_rows}/{existing_count} row(s) in Google BigQuery table "
                    f"{direction} using temporary table contains "
                    f"{keys} keys to delete."
                )

            finally:
                
                try:
                    print(
                        "🔄 [PLUGIN] Deleting temporary table "
                        f"{temp_table}..."
                    )                 
                    
                    self.client.query(f"DROP TABLE `{temp_table}`").result()
                    
                    print(
                        "✅ [PLUGIN] Successfully deleted temporary table "
                        f"{temp_table}."
                    )
                
                except Exception as e:
                    raise RuntimeError (
                        "❌ [PLUGIN] Failed to delete temporary table "
                        f"{temp_table} due to "
                        f"{e}."
                    )

            return

        raise ValueError(
            "❌ [PLUGIN] Failed to apply conflict handling for Google BigQuery table "
            f"{direction} due to unsupported conflict handling mode "
            f"{mode}."
        )
    
    # 1.3.8. Write table data
    def _write_table_data(
        self,
        *,
        df: pd.DataFrame,
        direction: str,
    ) -> None:
        
        try:
            print(
                "🔍 [PLUGIN] Writing data into Google BigQuery table "
                f"{direction} using default WRITE_APPEND mode..."
            )

            job = self.client.load_table_from_dataframe(
                df,
                direction,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND"
                ),
            )
            job.result()
            written_rows = job.output_rows or 0

            print(
                "✅ [PLUGIN] Successfully written "
                f"{written_rows}/{len(df)} row(s) to Google BigQuery table "
                f"{direction} direction with WRITE_APPEND mode."
            )

        except Exception as e:
            raise RuntimeError(
                "❌ [PLUGIN] Failed to write data into Google BigQuery table "
                f"{direction} due to "
                f"{str(e)}."
            )