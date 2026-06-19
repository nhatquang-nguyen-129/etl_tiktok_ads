import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from datetime import (
    date,
    datetime
)
import pandas as pd
import uuid

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

class internalGoogleBigqueryLoader:
    """
    Internal Google BigQuery Loader
    ---
    Principles:
        1. Initialize BigQuery client
        2. Check dataset existence
        3. Check table existence
        4. Apply INSERT/UPSERT DML
        5. Writer data into table
    ---
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
                f"{full_dataset_id} then dataset creation will be proceeding..."
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
    def _infer_df_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:

        print(
            "🔄 [PLUGIN] Inferring DataFrame schema for "
            f"{len(df)} row(s)..."
        )

        schema = []

        for col in df.columns:

            series = df[col]
            
            dtype = series.dtype

            print(
                "🔄 [PLUGIN] Inferring schema for column "
                f"{col} to dtype "
                f"{dtype}..."
            )

            try:

                # Priority strings ID columns
                if col.lower().endswith("_id") or col.lower() in ["id"]:

                    print(
                        f"⚠️ [PLUGIN] Column {col} detected as ID field then forcing to STRING type..."
                    )

                    bq_type = "STRING"

                elif pd.api.types.is_integer_dtype(dtype):
                    
                    bq_type = "INT64"

                elif pd.api.types.is_float_dtype(dtype):
                    
                    bq_type = "FLOAT64"

                elif pd.api.types.is_bool_dtype(dtype):
                    
                    bq_type = "BOOL"

                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    
                    bq_type = "TIMESTAMP"

                else:
                    
                    non_null = series.dropna()

                    if non_null.empty:
                    
                        bq_type = "STRING"

                    else:
                        
                        # Get random DataFrame sample
                        if len(non_null) <= 200:
                            
                            sample = non_null
                        
                        else:
                        
                            head_sample = non_null.head(50)
                        
                            tail_sample = non_null.tail(50)
                        
                            random_sample = non_null.sample(n=100, random_state=42)
                        
                            sample = pd.concat([head_sample, tail_sample, random_sample])

                        if sample.map(lambda x: isinstance(x, date) and not isinstance(x, datetime)).all():

                            bq_type = "DATE"

                        elif sample.map(lambda x: isinstance(x, (datetime, pd.Timestamp))).all():

                            if sample.map(lambda x: x.hour == 0 and x.minute == 0 and x.second == 0).all():

                                bq_type = "DATE"

                            else:

                                bq_type = "TIMESTAMP"

                        # String handling
                        elif sample.map(lambda x: isinstance(x, str)).all():

                            sample_str = sample.astype(str).str.strip()

                            sample_str = sample_str.replace("", pd.NA).dropna()

                            if sample_str.empty:
                                
                                bq_type = "STRING"

                            else:

                                if sample_str.str.match(r"^\d{4}-\d{2}-\d{2}$").all():

                                    bq_type = "DATE"

                                elif sample_str.str.match(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2})$").all():

                                    bq_type = "TIMESTAMP"

                                else:

                                    # Infer FLOAT if explicit decimal pattern
                                    if sample_str.str.contains(r"\.").any():
                                        
                                        try:
                                        
                                            sample_str.astype(float)
                                        
                                            bq_type = "FLOAT64"
                                        
                                        except:
                                        
                                            bq_type = "STRING"
                                    else:
                                        
                                        bq_type = "STRING"
                        else:

                            bq_type = "STRING"

                print(
                    f"✅ [PLUGIN] Successfully inferred schema for column "
                    f"{col} from dtype "
                    f"{dtype} to Google BigQuery type "
                    f"{bq_type}."
                )

                schema.append(
                    bigquery.SchemaField(
                        col, 
                        bq_type
                    )
                )

            except Exception as e:
                raise RuntimeError(
                    "❌ [PLUGIN] Failed to infer schema for column "
                    f"{col} with dtype "
                    f"{dtype} due to "
                    f"{e}."
                ) from e

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
                "⚠️ [PLUGIN] Failed to find Google BigQuery table "
                f"{direction} then table creation will be proceeding..."
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

            try:
                
                schema = self._infer_df_schema(df)
            
            except Exception as e:
            
                raise RuntimeError(
                    "❌ [PLUGIN] Failed to create Google BigQuery table due to schema inference failure."
                ) from e

            print(
                "🔍 [PLUGIN] Creating Google BigQuery table "
                f"{direction} with partition on "
                f"{partition} and cluster on "
                f"{cluster}..."
            )

            table = bigquery.Table(
                direction,
                schema=schema,
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
                f"{direction}."
            )
        
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
                f"{direction} with {keys} key(s)..."
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

                table = self.client.get_table(direction)

                schema_map = {field.name: field.field_type for field in table.schema}

                bq_type = schema_map.get(key)

                if not bq_type:

                    raise ValueError(
                        "❌ [PLUGIN] Failed to find key "
                        f"{key} not found in Google BigQuery schema."
                    )

                values = df_to_delete[key].dropna().tolist()

                if not values:

                    return

                try:
                    
                    if bq_type == "INT64":

                        values = [int(v) for v in values]

                    elif bq_type == "FLOAT64":

                        values = [float(v) for v in values]

                    elif bq_type == "BOOL":

                        values = [
                            v if isinstance(v, bool)
                            else str(v).lower() in ["true", "1"]
                            for v in values
                        ]

                    elif bq_type == "STRING":

                        values = [str(v) for v in values]

                    elif bq_type == "DATE":

                        values = [
                            pd.to_datetime(v).date()
                            for v in values
                        ]

                    elif bq_type == "TIMESTAMP":

                        values = [
                            pd.Timestamp(v).to_pydatetime()
                            for v in values
                        ]

                except Exception as e:

                    raise RuntimeError(
                        "❌ [PLUGIN] Failed to normalize values for key "
                        f"{key} due to "
                        f"{e}."
                    )

                print(
                    "🔍 [PLUGIN] Deleting existing row(s) in Google BigQuery table "
                    f"{direction} using key "
                    f"{key}..."
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
                    f"{deleted_rows} row(s) in Google BigQuery table "
                    f"{direction} using parameterized query with key {key}."
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
                        f"{df_to_delete[k].dtype} in temporary table to "
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