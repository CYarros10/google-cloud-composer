"""
Example Airflow DAG for Google Cloud Storage GCSFileTransformOperator operator.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSFileTransformOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_gcs_transform"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
TRANSFORM_SCRIPT_PATH = str(Path(__file__).parent / "resources" / "transform_script.py")
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
    description=" Airflow DAG for GCS File Transformation (Python)",
    tags=["demo", "google_cloud", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=UPLOAD_FILE_PATH, dst=FILE_NAME, bucket=BUCKET_NAME
    )
    transform_file = GCSFileTransformOperator(
        task_id="transform_file",
        source_bucket=BUCKET_NAME,
        source_object=FILE_NAME,
        transform_script=["python", TRANSFORM_SCRIPT_PATH],
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    create_bucket >> upload_file >> transform_file >> delete_bucket
    transform_file >> end
