"""
Example Airflow DAG that creates and deletes Bigquery data transfer configurations.
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_bigquery_dts"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"
FILE_NAME = "demo_bq_us-states.csv"
CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / FILE_NAME)
BUCKET_URI = f"gs://{BUCKET_NAME}/{FILE_NAME}"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
DTS_BQ_TABLE = "DTS_BQ_TABLE"
TRANSFER_CONFIG = {
    "destination_dataset_id": DATASET_NAME,
    "display_name": "test data transfer",
    "data_source_id": "google_cloud_storage",
    "schedule_options": {"disable_auto_scheduling": True},
    "params": {
        "field_delimiter": ",",
        "max_bad_records": "0",
        "skip_leading_rows": "1",
        "data_path_template": BUCKET_URI,
        "destination_table_name_template": DTS_BQ_TABLE,
        "file_format": "CSV",
    },
}
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
    description="This Airflow DAG automates the creation, transfer, and deletion of data between GCS and BigQuery.",
    tags=["demo", "google_cloud", "bigquery", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=FILE_LOCAL_PATH, dst=FILE_NAME, bucket=BUCKET_NAME
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=DTS_BQ_TABLE,
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
    )
    gcp_bigquery_create_transfer = BigQueryCreateDataTransferOperator(
        transfer_config=TRANSFER_CONFIG,
        project_id=PROJECT_ID,
        task_id="gcp_bigquery_create_transfer",
    )
    transfer_config_id = cast(
        str, XComArg(gcp_bigquery_create_transfer, key="transfer_config_id")
    )
    gcp_bigquery_start_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="gcp_bigquery_start_transfer",
        project_id=PROJECT_ID,
        transfer_config_id=transfer_config_id,
        requested_run_time={"seconds": int(time.time() + 60)},
    )
    gcp_run_sensor = BigQueryDataTransferServiceTransferRunSensor(
        task_id="gcp_run_sensor",
        transfer_config_id=transfer_config_id,
        run_id=cast(str, XComArg(gcp_bigquery_start_transfer, key="run_id")),
        expected_statuses={"SUCCEEDED"},
    )
    gcp_bigquery_delete_transfer = BigQueryDeleteDataTransferConfigOperator(
        transfer_config_id=transfer_config_id, task_id="gcp_bigquery_delete_transfer"
    )
    gcp_bigquery_delete_transfer.trigger_rule = TriggerRule.ALL_DONE
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
        >> upload_file
        >> create_dataset
        >> create_table
        >> gcp_bigquery_create_transfer
        >> gcp_bigquery_start_transfer
        >> gcp_run_sensor
        >> gcp_bigquery_delete_transfer
        >> [delete_dataset, delete_bucket, end]
    )
