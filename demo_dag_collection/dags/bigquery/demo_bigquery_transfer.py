"""
Example Airflow DAG for Google BigQuery service.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_bigquery_transfer"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
ORIGIN = "origin"
TARGET = "target"
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=55),
    },
    description="This Airflow DAG demonstrates BigQuery functionalities including dataset and table creation/deletion, data transfer between BigQuery tables, and data export to GCS.",
    tags=["demo", "google_cloud", "bigquery", "transfer"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    create_origin_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{ORIGIN}_table",
        dataset_id=DATASET_NAME,
        table_id=ORIGIN,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    create_target_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{TARGET}_table",
        dataset_id=DATASET_NAME,
        table_id=TARGET,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    copy_selected_data = BigQueryToBigQueryOperator(
        task_id="copy_selected_data",
        source_project_dataset_tables=f"{DATASET_NAME}.{ORIGIN}",
        destination_project_dataset_table=f"{DATASET_NAME}.{TARGET}",
    )
    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table=f"{DATASET_NAME}.{ORIGIN}",
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/export-bigquery.csv"],
        force_rerun=True
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_bucket
        >> create_dataset
        >> create_origin_table
        >> create_target_table
        >> copy_selected_data
        >> bigquery_to_gcs
        >> [delete_dataset, delete_bucket]
    )
    bigquery_to_gcs >> end
