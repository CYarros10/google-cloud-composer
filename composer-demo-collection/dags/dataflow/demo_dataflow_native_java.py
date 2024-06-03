"""
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Java.

Important Note:
    This test downloads Java JAR file from the public bucket. In case the JAR file cannot be downloaded
    or is not compatible with the Java version used in the test, the source code for this test can be
    downloaded from here (https://beam.apache.org/get-started/wordcount-example) and needs to be compiled
    manually in order to work.

    You can follow the instructions on how to pack a self-executing jar here:
    https://beam.apache.org/documentation/runners/dataflow/

Requirements:
    These operators require the gcloud command and Java's JRE to run.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import CheckJobRunning
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataflow_native_java"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
PUBLIC_BUCKET = "airflow-system-tests-resources"
JAR_FILE_NAME = "word-count-beam-bundled-0.1.jar"
REMOTE_JAR_FILE_PATH = f"dataflow/java/{JAR_FILE_NAME}"
GCS_OUTPUT = f"gs://{BUCKET_NAME}"
GCS_JAR = f"gs://{PUBLIC_BUCKET}/dataflow/java/{JAR_FILE_NAME}"
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
    },
    description="This is an Airflow DAG for running Java-based Apache Beam word count pipelines on Cloud Dataflow.",
    tags=["demo", "google_cloud", "dataflow", "java", "beam"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=REMOTE_JAR_FILE_PATH,
        bucket=PUBLIC_BUCKET,
        filename=JAR_FILE_NAME,
    )
    start_java_job = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start-java-job",
        jar=GCS_JAR,
        pipeline_options={"output": GCS_OUTPUT},
        job_class="org.apache.beam.examples.WordCount",
        dataflow_config={
            "check_if_running": CheckJobRunning.IgnoreJob,
            "location": LOCATION_REGION,
            "poll_sleep": 10,
        },
    )
    start_java_deferrable = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start-java-job-deferrable",
        jar=GCS_JAR,
        pipeline_options={"output": GCS_OUTPUT},
        job_class="org.apache.beam.examples.WordCount",
        dataflow_config={
            "check_if_running": CheckJobRunning.WaitForRun,
            "location": LOCATION_REGION,
            "poll_sleep": 10,
            "append_job_name": False,
        },
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_bucket
        >> download_file
        >> [start_java_job, start_java_deferrable]
        >> delete_bucket
    )
    [start_java_job, start_java_deferrable] >> end
