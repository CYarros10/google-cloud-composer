from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
GCP_PROJECT_ID = "your-project"
DAG_ID = "demo_s3_to_gcs"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
GCS_BUCKET_URL = f"gs://{BUCKET_NAME}/"
UPLOAD_FILE = "/tmp/example-file.txt"
PREFIX = "TESTS"


@task(task_id="upload_file_to_s3")
def upload_file():
    """A callable to upload file to AWS bucket"""
    s3_hook = S3Hook()
    s3_hook.load_file(filename=UPLOAD_FILE, key=PREFIX, bucket_name=BUCKET_NAME)


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
    description="This DAG creates S3 and GCS buckets, transfers a file from S3 to GCS, then deletes both buckets.",
    tags=["demo", "google_cloud", "gcs", "aws", "s3"],
) as dag:
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=BUCKET_NAME, region_name="us-east-1"
    )
    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=GCP_PROJECT_ID
    )
    transfer_to_gcs = S3ToGCSOperator(
        task_id="s3_to_gcs_task",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        dest_gcs=GCS_BUCKET_URL,
        deferrable=True,
    )
    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=BUCKET_NAME,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_gcs_bucket = GCSDeleteBucketOperator(
        task_id="delete_gcs_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_gcs_bucket
        >> create_s3_bucket
        >> upload_file()
        >> transfer_to_gcs
        >> delete_s3_bucket
        >> delete_gcs_bucket
    )
    transfer_to_gcs >> end
