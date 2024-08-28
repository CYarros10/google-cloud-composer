"""
Example Airflow DAG for Google Cloud Storage sensors.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceAsyncSensor,
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_gcs_sensor"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)


def workaround_in_debug_executor(cls):
    """
    DebugExecutor change sensor mode from poke to reschedule. Some sensors don't work correctly
    in reschedule mode. They are decorated with `poke_mode_only` decorator to fail when mode is changed.
    This method creates dummy property to overwrite it and force poke method to always return True.
    """
    cls.mode = dummy_mode_property()
    cls.poke = lambda self, context: True


def dummy_mode_property():

    def mode_getter(self):
        return self._mode

    def mode_setter(self, value):
        self._mode = value

    return property(mode_getter, mode_setter)


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
    description="This Airflow DAG monitors a GCS bucket for object existence, updates, and upload session completion.",
    tags=["demo", "google_cloud", "gcs", "sensors"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    workaround_in_debug_executor(GCSUploadSessionCompleteSensor)
    gcs_upload_session_complete = GCSUploadSessionCompleteSensor(
        bucket=BUCKET_NAME,
        prefix=FILE_NAME,
        inactivity_period=15,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_complete_task",
    )
    gcs_upload_session_async_complete = GCSUploadSessionCompleteSensor(
        bucket=BUCKET_NAME,
        prefix=FILE_NAME,
        inactivity_period=15,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_async_complete",
        deferrable=True,
    )
    gcs_update_object_exists = GCSObjectUpdateSensor(
        bucket=BUCKET_NAME, object=FILE_NAME, task_id="gcs_object_update_sensor_task"
    )
    gcs_update_object_exists_async = GCSObjectUpdateSensor(
        bucket=BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_update_sensor_task_async",
        deferrable=True,
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=UPLOAD_FILE_PATH, dst=FILE_NAME, bucket=BUCKET_NAME
    )
    gcs_object_exists = GCSObjectExistenceSensor(
        bucket=BUCKET_NAME, object=FILE_NAME, task_id="gcs_object_exists_task"
    )
    gcs_object_exists_async = GCSObjectExistenceAsyncSensor(
        bucket=BUCKET_NAME, object=FILE_NAME, task_id="gcs_object_exists_task_async"
    )
    gcs_object_exists_defered = GCSObjectExistenceSensor(
        bucket=BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_exists_defered",
        deferrable=True,
    )
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_NAME,
        prefix=FILE_NAME[:5],
        task_id="gcs_object_with_prefix_exists_task",
    )
    gcs_object_with_prefix_exists_async = GCSObjectsWithPrefixExistenceSensor(
        bucket=BUCKET_NAME,
        prefix=FILE_NAME[:5],
        task_id="gcs_object_with_prefix_exists_task_async",
        deferrable=True,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    chain(
        create_bucket,
        upload_file,
        [
            gcs_object_exists,
            gcs_object_exists_defered,
            gcs_object_exists_async,
            gcs_object_with_prefix_exists,
            gcs_object_with_prefix_exists_async,
        ],
        delete_bucket,
    )
    chain(
        create_bucket,
        gcs_upload_session_complete,
        gcs_update_object_exists,
        gcs_update_object_exists_async,
        delete_bucket,
    )
    [gcs_object_with_prefix_exists_async, gcs_update_object_exists_async] >> end
