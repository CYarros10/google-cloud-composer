"""
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Asynchronous Python.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataflow_native_python_async"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
PYTHON_FILE_NAME = "demo_df_wordcount_debugging.txt"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
GCS_PYTHON_SCRIPT = f"gs://{BUCKET_NAME}/{PYTHON_FILE_NAME}"
PYTHON_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / PYTHON_FILE_NAME)
LOCATION_REGION = "us-central1"
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
        "dataflow_default_options": {
            "tempLocation": GCS_TMP,
            "stagingLocation": GCS_STAGING,
        },
    },
    description='The DataflowJobStatus sensor waits for the Dataflow job to complete, the DataflowJobMetrics sensor waits for the \'Service-cpu_num_seconds\' metric to reach a value of 100 or more, the DataflowJobMessages sensor waits for a specific message in the job logs containing "Adding workflow start and stop steps.", and the DataflowJobAutoScalingEvents sensor waits for a specific autoscaling event containing "Worker pool started.".',
    tags=["demo", "google_cloud", "dataflow", "python", "beam"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=PYTHON_FILE_LOCAL_PATH,
        dst=PYTHON_FILE_NAME,
        bucket=BUCKET_NAME,
    )
    start_python_job_async = BeamRunPythonPipelineOperator(
        task_id="start_python_job_async",
        runner=BeamRunnerType.DataflowRunner,
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={"output": GCS_OUTPUT},
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={
            "job_name": "start_python_job_async",
            "location": LOCATION_REGION,
            "wait_until_finished": False,
        },
    )
    wait_for_python_job_async_done = DataflowJobStatusSensor(
        task_id="wait_for_python_job_async_done",
        job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
        location=LOCATION_REGION,
    )

    def check_metric_scalar_gte(metric_name: str, value: int) -> Callable:
        """Check is metric greater than equals to given value."""

        def callback(metrics: list[dict]) -> bool:
            dag.log.info("Looking for '%s' >= %d", metric_name, value)
            for metric in metrics:
                context = metric.get("name", {}).get("context", {})
                original_name = context.get("original_name", "")
                tentative = context.get("tentative", "")
                if original_name == "Service-cpu_num_seconds" and (not tentative):
                    return metric["scalar"] >= value
            raise AirflowException(f"Metric '{metric_name}' not found in metrics")

        return callback

    wait_for_python_job_async_metric = DataflowJobMetricsSensor(
        task_id="wait_for_python_job_async_metric",
        job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
        location=LOCATION_REGION,
        callback=check_metric_scalar_gte(
            metric_name="Service-cpu_num_seconds", value=100
        ),
        fail_on_terminal_state=False,
    )

    def check_message(messages: list[dict]) -> bool:
        """Check message"""
        for message in messages:
            if "Adding workflow start and stop steps." in message.get(
                "messageText", ""
            ):
                return True
        return False

    wait_for_python_job_async_message = DataflowJobMessagesSensor(
        task_id="wait_for_python_job_async_message",
        job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
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

    wait_for_python_job_async_autoscaling_event = DataflowJobAutoScalingEventsSensor(
        task_id="wait_for_python_job_async_autoscaling_event",
        job_id="{{task_instance.xcom_pull('start_python_job_async')['dataflow_job_id']}}",
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
        >> start_python_job_async
        >> [
            wait_for_python_job_async_done,
            wait_for_python_job_async_metric,
            wait_for_python_job_async_message,
            wait_for_python_job_async_autoscaling_event,
        ]
        >> delete_bucket
    )
    [
        wait_for_python_job_async_done,
        wait_for_python_job_async_metric,
        wait_for_python_job_async_message,
        wait_for_python_job_async_autoscaling_event,
    ] >> end
