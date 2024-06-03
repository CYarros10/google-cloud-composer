"""
Example Airflow DAG for testing interaction between Google Cloud Storage and local file system.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gcs_upload_download"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
PATH_TO_SAVED_FILE = "example_upload_download.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
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
    description="This Airflow DAG tests interaction between Google Cloud Storage and the local file system in 7 words.\n",
    tags=["demo", "google_cloud", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=UPLOAD_FILE_PATH, dst=FILE_NAME, bucket=BUCKET_NAME
    )
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=FILE_NAME,
        bucket=BUCKET_NAME,
        filename=PATH_TO_SAVED_FILE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME
    )
    end = EmptyOperator(task_id="end")
    delete_bucket.trigger_rule = TriggerRule.ALL_DONE
    create_bucket >> upload_file >> download_file >> delete_bucket
    download_file >> end
