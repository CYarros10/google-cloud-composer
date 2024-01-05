"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
from airflow.utils.task_group import TaskGroup

# ----- Google Cloud Storage Airflow Imports
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator

from airflow.providers.google.cloud.operators.gcs import (
    GCSDeleteObjectsOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)

from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceAsyncSensor,
    GCSObjectsWithPrefixExistenceSensor,
)


# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

# -------------------------
# Tags, Default Args, and Macros
# -------------------------
tags = ["application:samples"]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2022, 3, 15),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}
user_defined_macros = {
    "project_id": "cy-artifacts",
    "region": "us-central1",
    "gcs_bucket_name": "cy-artifacts-health-check-bucket",
    "gcs_location": "us-central1",
    "gcs_path": "health_check_path",
    "gcs_object": "test_upload_object",
    "gcs_download_bucket": "cy-artifacts-airflow-files",
    "gcs_download_obj": "sample.txt",
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"cloudstorage_dag_{VERSION}",
    description="Sample DAG for various Cloud Storage tasks.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    # -------------------------------------------------------------------------
    # Google Cloud Storage
    # -------------------------------------------------------------------------

    with TaskGroup(group_id="googlecloudstorage_tg1") as googlecloudstorage_tg1:
        pre_gcs_delete_bucket = GCSDeleteBucketOperator(
            task_id="pre_gcs_delete_bucket",
            bucket_name="{{gcs_bucket_name}}",
            force=True,
        )

        gcs_create_bucket = GCSCreateBucketOperator(
            task_id="gcs_create_bucket",
            bucket_name="{{gcs_bucket_name}}",
            storage_class="STANDARD",
            location="{{gcs_location}}",
            labels={"use-case": "demo"},
            project_id="{{project_id}}",
            trigger_rule="all_done",
        )

        ## CREATE FILE HERE TO UPLOAD

        gcs_upload_file = LocalFilesystemToGCSOperator(
            task_id="gcs_upload_file",
            src="test_download_object",
            dst="{{gcs_path}}/{{gcs_object}}",
            bucket="{{gcs_bucket_name}}",
        )

        gcs_download_file = GCSToLocalFilesystemOperator(
            task_id="gcs_download_file",
            bucket="{{gcs_download_bucket}}",
            object_name="{{gcs_download_obj}}",
            filename="test_download_object",
        )

        gcs_list_objects = GoogleCloudStorageListOperator(
            task_id="gcs_list_objects",
            bucket="{{gcs_bucket_name}}",
            prefix="{{gcs_path}}",
        )

        gcs_object_exists_async = GCSObjectExistenceAsyncSensor(
            task_id="gcs_object_exists_task_async",
            bucket="{{gcs_bucket_name}}",
            object="{{gcs_path}}/{{gcs_object}}",
        )

        gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
            task_id="gcs_object_with_prefix_exists_task",
            bucket="{{gcs_bucket_name}}",
            prefix="{{gcs_path}}/",
        )

        gcs_delete_objects = GCSDeleteObjectsOperator(
            task_id="gcs_delete_objects",
            bucket_name="{{gcs_bucket_name}}",
            prefix="{{gcs_path}}/",
            trigger_rule="all_done",
        )

        post_gcs_delete_bucket = GCSDeleteBucketOperator(
            task_id="post_gcs_delete_bucket",
            bucket_name="{{gcs_bucket_name}}",
            force=True,
            trigger_rule="all_done",
        )

        (
            pre_gcs_delete_bucket
            >> gcs_create_bucket
            >> gcs_download_file
            >> gcs_upload_file
            >> gcs_list_objects
            >> gcs_object_exists_async
            >> gcs_object_with_prefix_exists
            >> gcs_delete_objects
            >> post_gcs_delete_bucket
        )
