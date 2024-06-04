"""
Example Airflow DAG for Google Cloud Storage operators for listing, copying and deleting
bucket content.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSListObjectsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gcs_copy_delete"
BUCKET_NAME_SRC = f"bucket_{DAG_ID}_{ENV_ID}"
BUCKET_NAME_DST = f"bucket_dst_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
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
    description="This DAG creates buckets, lists objects, copies files, deletes objects, and deletes buckets.",
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
        dst=FILE_NAME,
        bucket=BUCKET_NAME_SRC,
    )
    list_buckets = GCSListObjectsOperator(
        task_id="list_buckets", bucket=BUCKET_NAME_SRC
    )
    list_buckets_result = BashOperator(
        task_id="list_buckets_result", bash_command=f"echo {list_buckets.output}"
    )
    copy_file = GCSToGCSOperator(
        task_id="copy_file",
        source_bucket=BUCKET_NAME_SRC,
        source_object=FILE_NAME,
        destination_bucket=BUCKET_NAME_DST,
        destination_object=FILE_NAME,
    )
    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files", bucket_name=BUCKET_NAME_SRC, objects=[FILE_NAME]
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
        list_buckets,
        list_buckets_result,
        copy_file,
        delete_files,
        [delete_bucket_src, delete_bucket_dst],
    )
    delete_files >> end
