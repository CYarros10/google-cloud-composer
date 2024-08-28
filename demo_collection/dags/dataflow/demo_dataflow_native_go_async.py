"""
Example Airflow DAG for Apache Beam operators

Requirements:
    This test requires the gcloud and go commands to run.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunGoPipelineOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
LOCATION_REGION = "us-central1"
DAG_ID = "demo_dataflow_native_go_async"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
GO_FILE_NAME = "demo_df_wordcount.go"
GO_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / GO_FILE_NAME)
GCS_GO = f"gs://{BUCKET_NAME}/{GO_FILE_NAME}"
with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
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
        "dataflow_default_options": {
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
        },
    },
    description="This Airflow DAG demonstrates asynchronous execution of a Go Apache Beam pipeline on Cloud Dataflow.",
    tags=["demo", "google_cloud", "dataflow", "go", "beam"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=GO_FILE_LOCAL_PATH,
        dst=GO_FILE_NAME,
        bucket=BUCKET_NAME,
    )
    start_go_pipeline_dataflow_runner = BeamRunGoPipelineOperator(
        task_id="start_go_pipeline_dataflow_runner",
        runner=BeamRunnerType.DataflowRunner,
        go_file=GCS_GO,
        pipeline_options={
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
            "output": GCS_OUTPUT,
            "WorkerHarnessContainerImage": "apache/beam_go_sdk:2.46.0",
        },
        dataflow_config=DataflowConfiguration(
            job_name="start_go_job", location=LOCATION_REGION
        ),
    )
    wait_for_go_job_async_done = DataflowJobStatusSensor(
        task_id="wait_for_go_job_async_done",
        job_id="{{task_instance.xcom_pull('start_go_pipeline_dataflow_runner')['dataflow_job_id']}}",
        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
        location=LOCATION_REGION,
    )

    def check_message(messages: list[dict]) -> bool:
        """Check message"""
        for message in messages:
            if "Adding workflow start and stop steps." in message.get(
                "messageText", ""
            ):
                return True
        return False

    wait_for_go_job_async_message = DataflowJobMessagesSensor(
        task_id="wait_for_go_job_async_message",
        job_id="{{task_instance.xcom_pull('start_go_pipeline_dataflow_runner')['dataflow_job_id']}}",
        location=LOCATION_REGION,
        callback=check_message,
        fail_on_terminal_state=False,
    )

    def check_autoscaling_event(autoscaling_events: list[dict]) -> bool:
        """Check autoscaling event"""
        for autoscaling_event in autoscaling_events:
            if "Worker pool started." in autoscaling_event.get("description", {}).get(
                "messageText", ""
            ):
                return True
        return False

    wait_for_go_job_async_autoscaling_event = DataflowJobAutoScalingEventsSensor(
        task_id="wait_for_go_job_async_autoscaling_event",
        job_id="{{task_instance.xcom_pull('start_go_pipeline_dataflow_runner')['dataflow_job_id']}}",
        location=LOCATION_REGION,
        callback=check_autoscaling_event,
        fail_on_terminal_state=False,
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
        >> start_go_pipeline_dataflow_runner
        >> [
            wait_for_go_job_async_done,
            wait_for_go_job_async_message,
            wait_for_go_job_async_autoscaling_event,
        ]
        >> delete_bucket
    )
    [
        wait_for_go_job_async_done,
        wait_for_go_job_async_message,
        wait_for_go_job_async_autoscaling_event,
    ] >> end
