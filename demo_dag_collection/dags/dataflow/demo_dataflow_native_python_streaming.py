"""
Example Airflow DAG for testing Google Dataflow Beam Pipeline Operator with Python for Streaming job.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStopJobOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateTopicOperator,
    PubSubDeleteTopicOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataflow_native_python_streaming"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_PYTHON_SCRIPT = (
    f"gs://{RESOURCE_DATA_BUCKET}/dataflow/python/streaming_wordcount.py"
)
LOCATION_REGION = "us-central1"
TOPIC_ID = f"topic-{DAG_ID}"
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
    description="The Airflow DAG demonstrates a streaming Python Apache Beam pipeline that uses a public Pub/Sub topic for input and a customized one for output.",
    tags=["demo", "google_cloud", "dataflow", "python", "beam", "streaming"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    create_pub_sub_topic = PubSubCreateTopicOperator(
        task_id="create_topic",
        topic=TOPIC_ID,
        project_id=PROJECT_ID,
        fail_if_exists=False,
    )
    start_streaming_python_job = BeamRunPythonPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="start_streaming_python_job",
        py_file=GCS_PYTHON_SCRIPT,
        py_options=[],
        pipeline_options={
            "temp_location": GCS_TMP,
            "input_topic": "projects/pubsub-public-data/topics/taxirides-realtime",
            "output_topic": f"projects/{PROJECT_ID}/topics/{TOPIC_ID}",
            "streaming": True,
        },
        py_requirements=["apache-beam[gcp]==2.46.0", "cmake"],
        py_interpreter="python3",
        py_system_site_packages=False,
        dataflow_config={"location": LOCATION_REGION},
    )
    stop_dataflow_job = DataflowStopJobOperator(
        task_id="stop_dataflow_job",
        location=LOCATION_REGION,
        job_id="{{ task_instance.xcom_pull(task_ids='start_streaming_python_job')['dataflow_job_id'] }}",
    )
    delete_topic = PubSubDeleteTopicOperator(
        task_id="delete_topic", topic=TOPIC_ID, project_id=PROJECT_ID
    )
    delete_topic.trigger_rule = TriggerRule.ALL_DONE
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_bucket
        >> create_pub_sub_topic
        >> start_streaming_python_job
        >> stop_dataflow_job
        >> delete_topic
        >> delete_bucket
    )
    stop_dataflow_job >> end
