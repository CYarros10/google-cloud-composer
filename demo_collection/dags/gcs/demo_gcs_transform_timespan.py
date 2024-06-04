"""
Example Airflow DAG for Google Cloud Storage GCSTimeSpanFileTransformOperator operator.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSTimeSpanFileTransformOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gcs_transform_timespan"
BUCKET_NAME_SRC = f"bucket_{DAG_ID}_{ENV_ID}"
BUCKET_NAME_DST = f"bucket_dst_{DAG_ID}_{ENV_ID}"
SOURCE_GCP_CONN_ID = DESTINATION_GCP_CONN_ID = "google_cloud_default"
FILE_NAME = "example_upload.txt"
SOURCE_PREFIX = "timespan_source"
DESTINATION_PREFIX = "timespan_destination"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
TRANSFORM_SCRIPT_PATH = str(
    Path(__file__).parent / "resources" / "transform_timespan.py"
)
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
    description="This Airflow DAG uses the GCSTimeSpanFileTransformOperator to transform files in GCS based on a specified time span.",
    tags=["demo", "google_cloud", "gcs"],
) as dag:
    create_bucket_src = GCSCreateBucketOperator(
        task_id="create_bucket_src", bucket_name=BUCKET_NAME_SRC, project_id=PROJECT_ID
    )
    create_bucket_dst = GCSCreateBucketOperator(
        task_id="create_bucket_dst", bucket_name=BUCKET_NAME_DST, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=f"{SOURCE_PREFIX}/{FILE_NAME}",
        bucket=BUCKET_NAME_SRC,
    )
    gcs_timespan_transform_files_task = GCSTimeSpanFileTransformOperator(
        task_id="gcs_timespan_transform_files",
        source_bucket=BUCKET_NAME_SRC,
        source_prefix=SOURCE_PREFIX,
        source_gcp_conn_id=SOURCE_GCP_CONN_ID,
        destination_bucket=BUCKET_NAME_DST,
        destination_prefix=DESTINATION_PREFIX,
        destination_gcp_conn_id=DESTINATION_GCP_CONN_ID,
        transform_script=["python", TRANSFORM_SCRIPT_PATH],
    )
    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src",
        bucket_name=BUCKET_NAME_SRC,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket_dst = GCSDeleteBucketOperator(
        task_id="delete_bucket_dst",
        bucket_name=BUCKET_NAME_DST,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    chain(
        [create_bucket_src, create_bucket_dst],
        upload_file,
        gcs_timespan_transform_files_task,
        [delete_bucket_src, delete_bucket_dst],
    )
    gcs_timespan_transform_files_task >> end
