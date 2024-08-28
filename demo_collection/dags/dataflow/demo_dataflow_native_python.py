"""
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Python.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_dataflow_native_python"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
PYTHON_FILE_NAME = "demo_df_wordcount_debugging.py"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
GCS_PYTHON_SCRIPT = f"gs://{BUCKET_NAME}/{PYTHON_FILE_NAME}"
PYTHON_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / PYTHON_FILE_NAME)
LOCATION_REGION = "us-central1"
default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}
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
    description="Airflow DAG to run and stop two Native Python Dataflow pipelines, uploading a script, creating and then deleting a bucket.",
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
    start_python_job = BeamRunPythonPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_python_job",
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={"output": GCS_OUTPUT},
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={"location": LOCATION_REGION},
    )
    start_python_job_local = BeamRunPythonPipelineOperator(
        task_id="start_python_job_local",
        py_file="apache_beam.examples.wordcount",
        py_options=["-m"],
        pipeline_options={"output": GCS_OUTPUT},
        py_interpreter="python3",
        py_system_site_packages=False,
    )
    stop_dataflow_job = DataflowStopJobOperator(
        task_id="stop_dataflow_job",
        location=LOCATION_REGION,
        job_name_prefix="start-python-pipeline",
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
        >> start_python_job
        >> start_python_job_local
        >> stop_dataflow_job
        >> delete_bucket
    )
    stop_dataflow_job >> end
