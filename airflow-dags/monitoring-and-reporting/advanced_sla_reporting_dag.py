"""
A DAG that will pull dag_run and sla_miss metadata to generate an
advanced SLA reporting table.

## Usage

1. Update the global variables. BQ_PROJECT, BQ_DATASET, GCS_BUCKET,
   GCS_OBJECT_PATH

2. WRITE_DISPOSITON is set to WRITE_TRUNCATE the BigQuery location by default.
   This means that any data removed from Airflow metadata DB via UI or cleanup
   processes will also be removed from BigQuery location. If you change to
   WRITE_APPEND you'll need to manage duplicate data in another process.
   
3. Currently set to table schemas for Airflow 2.4.1

4. Put the DAG in your Google Cloud Storage bucket.
"""

from datetime import datetime, timedelta

from airflow import models
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)

default_args = {
    "owner": "auditing",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with models.DAG(
    f"advanced_sla_reporting_dag",
    description=f"Sample dag that pulls dag_run metadata and generates SLA info",
    is_paused_upon_creation=True,
    catchup=False,
    start_date=datetime(2023, 2, 5),
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=f"0 0 * * *",  # every day at midnight
) as dag:
    
    BQ_PROJECT = "<YOUR PROJECT CONTAINING BIGQUERY DATASET>"
    BQ_DATASET = "<YOUR BIGQUERY DATASET>"
    GCS_BUCKET = "<YOUR GOOGLE CLOUD STORAGE BUCKET>"
    GCS_OBJECT_PATH = "<DESIRED GOOGLE CLOUD STORAGE PATH TO LOAD DATA INTO>"

    WRITE_DISPOSITION = "WRITE_TRUNCATE"

    POSTGRES_CONNECTION_ID = "airflow_db"
    FILE_FORMAT = "csv"

    generate_sla_report_table = BigQueryInsertJobOperator(
        task_id="bq_insert_job",
        configuration={
            "query": {
                "query": """
  CREATE OR REPLACE TABLE
  {dataset}.advanced_sla_reporting AS
SELECT 
distinct
  dr.dag_id,
  dr.run_id dag_run_id,
  dr.execution_date dag_run_execution_date_ts,
  TIMESTAMP_SECONDS(CAST (dr.data_interval_start AS INT64)) dag_run_data_interval_start_ts,
  TIMESTAMP_SECONDS(CAST (dr.data_interval_end AS INT64)) dag_run_data_interval_end_ts,
  TIMESTAMP_SECONDS(CAST (dr.queued_at AS INT64)) dag_run_queue_ts,
  TIMESTAMP_SECONDS(CAST (dr.start_date AS INT64)) dag_run_start_ts,
  TIMESTAMP_SECONDS(CAST (dr.end_date AS INT64)) dag_run_end_ts,
  ROUND(dr.end_date - dr.start_date,4) as dag_run_duration_sec,
  s.execution_date sla_logical_date_ts,
  s.minimum_ts sla_miss_ts,
  TIMESTAMP_DIFF(s.minimum_ts, TIMESTAMP_SECONDS(CAST (dr.execution_date AS INT64)), MINUTE) as sla_miss_in_mins,
  TIMESTAMP_DIFF( s.minimum_ts, TIMESTAMP_SECONDS(CAST (dr.execution_date AS INT64)), HOUR) as sla_miss_in_hours,
  TIMESTAMP_DIFF( s.minimum_ts, TIMESTAMP_SECONDS(CAST (dr.execution_date AS INT64)), DAY) as sla_miss_in_days
FROM {dataset}.dag_run dr
JOIN 
( SELECT dag_id, execution_date, TIMESTAMP_SECONDS(CAST (MIN(timestamp) AS INT64)) as minimum_ts # prevent dupes by getting the first sla miss timestamp only
  FROM {dataset}.sla_miss  GROUP BY dag_id, execution_date
) s on s.dag_id = dr.dag_id and s.execution_date = dr.execution_date;
                """.format(
                    dataset=BQ_DATASET
                ),
                "useLegacySql": False,
            }
        },
        location="US",
    )

    SOURCE_TABLES = [
        "dag_run",
        "sla_miss"
    ]

    for table in SOURCE_TABLES:

        with TaskGroup(group_id=f"{table}_export") as export_tg:

            query = f"SELECT * FROM {table};"

            postgres_to_gcs = PostgresToGCSOperator(
                task_id="postgres_to_gcs",
                postgres_conn_id=POSTGRES_CONNECTION_ID,
                sql=query,
                bucket=GCS_BUCKET,
                filename=f"{GCS_OBJECT_PATH}/{table}/{table}.{FILE_FORMAT}",
                export_format=FILE_FORMAT,
                field_delimiter=",",
                gzip=False,
                use_server_side_cursor=False,
                execution_timeout=timedelta(minutes=15),
            )

            # will fail if table row count == 0 (no file will exist)
            output_check = GCSObjectsWithPrefixExistenceSensor(
                task_id="output_check",
                bucket=GCS_BUCKET,
                prefix=f"{GCS_OBJECT_PATH}/{table}/",
                mode="reschedule",
                poke_interval=60,
                timeout=60 * 3,
            )

            gcs_to_bq = GCSToBigQueryOperator(
                task_id="gcs_to_bq",
                bucket=GCS_BUCKET,
                source_objects=[f"{GCS_OBJECT_PATH}/{table}/{table}.{FILE_FORMAT}"],
                destination_project_dataset_table=".".join([BQ_PROJECT, BQ_DATASET, table]),
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=WRITE_DISPOSITION,
                skip_leading_rows=1,
                allow_quoted_newlines=True,
                field_delimiter=",",
                execution_timeout=timedelta(minutes=10),
            )

            table_check = BigQueryTableExistenceSensor(
                task_id="table_check",
                project_id=BQ_PROJECT,
                dataset_id=BQ_DATASET,
                table_id=table,
                mode="reschedule",
                poke_interval=60,
                timeout=60 * 3,
            )

            cleanup = GCSDeleteObjectsOperator(
                task_id="cleanup",
                bucket_name=GCS_BUCKET,
                prefix=f"{GCS_OBJECT_PATH}/{table}/",
                execution_timeout=timedelta(minutes=5),
            )

            postgres_to_gcs >> output_check >> gcs_to_bq >> table_check >> cleanup
        
        export_tg >> generate_sla_report_table
