"""
Example Airflow DAG for Google Cloud Storage ACL (Access Control List) operators.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gcs_acl"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
GCS_ACL_ENTITY = "allUsers"
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"
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
    description="This Airflow DAG creates a public GCS bucket, uploads a file, modifies ACLs, then deletes the bucket.",
    tags=["demo", "google_cloud", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        resource={"predefined_acl": "public_read_write"},
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=UPLOAD_FILE_PATH, dst=FILE_NAME, bucket=BUCKET_NAME
    )
    gcs_bucket_create_acl_entry_task = GCSBucketCreateAclEntryOperator(
        bucket=BUCKET_NAME,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_BUCKET_ROLE,
        task_id="gcs_bucket_create_acl_entry_task",
    )
    gcs_object_create_acl_entry_task = GCSObjectCreateAclEntryOperator(
        bucket=BUCKET_NAME,
        object_name=FILE_NAME,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_OBJECT_ROLE,
        task_id="gcs_object_create_acl_entry_task",
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
        >> gcs_bucket_create_acl_entry_task
        >> gcs_object_create_acl_entry_task
        >> delete_bucket
    )
    gcs_object_create_acl_entry_task >> end
