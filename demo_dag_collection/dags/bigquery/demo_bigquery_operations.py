"""
Example Airflow DAG for Google BigQuery service local file upload and external table creation.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
DAG_ID = "demo_bigquery_operations"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATA_SAMPLE_GCS_OBJECT_NAME = "bigquery/us-states/demo_bq_us-states.csv"
CSV_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / "demo_bq_us-states.csv")
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
    description="This Airflow DAG showcases the creation and deletion of a BigQuery dataset and a GCS bucket.",
    tags=["demo", "google_cloud", "bigquery"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=CSV_FILE_LOCAL_PATH,
        dst=DATA_SAMPLE_GCS_OBJECT_NAME,
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
    )
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        destination_project_dataset_table=f"{DATASET_NAME}.external_table",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        [create_bucket, create_dataset]
        >> upload_file
        >> create_external_table
        >> delete_dataset
        >> [delete_bucket, end]
    )
