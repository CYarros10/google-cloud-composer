"""
Example Airflow DAG for testing Google Dataflow
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator` operator.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"
DAG_ID = "demo_dataflow_template"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
CSV_FILE_NAME = "demo_df_input.csv"
AVRO_FILE_NAME = "demo_df_output.avro"
AVRO_SCHEMA = "demo_df_schema.json"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
PYTHON_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / CSV_FILE_NAME)
SCHEMA_LOCAL_PATH = str(Path(__file__).parent / "resources" / AVRO_SCHEMA)
BODY = {
    "launchParameter": {
        "jobName": "test-flex-template",
        "parameters": {
            "inputFileSpec": f"gs://{BUCKET_NAME}/{CSV_FILE_NAME}",
            "outputBucket": f"gs://{BUCKET_NAME}/output/{AVRO_FILE_NAME}",
            "outputFileFormat": "avro",
            "inputFileFormat": "csv",
            "schema": f"gs://{BUCKET_NAME}/{AVRO_SCHEMA}",
        },
        "environment": {},
        "containerSpecGcsPath": "gs://dataflow-templates/latest/flex/File_Format_Conversion",
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
    description="This DAG starts a templated Word Count job and a custom Flex Template job for file format conversion. ",
    tags=["demo", "google_cloud", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=PYTHON_FILE_LOCAL_PATH,
        dst=CSV_FILE_NAME,
        bucket=BUCKET_NAME,
    )
    upload_schema = LocalFilesystemToGCSOperator(
        task_id="upload_schema_to_bucket",
        src=SCHEMA_LOCAL_PATH,
        dst=AVRO_SCHEMA,
        bucket=BUCKET_NAME,
    )
    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start_template_job",
        project_id=PROJECT_ID,
        template="gs://dataflow-templates/latest/Word_Count",
        parameters={
            "inputFile": f"gs://{BUCKET_NAME}/{CSV_FILE_NAME}",
            "output": GCS_OUTPUT,
        },
        location=LOCATION_REGION,
    )
    start_flex_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_job",
        project_id=PROJECT_ID,
        body=BODY,
        location=LOCATION_REGION,
        append_job_name=False,
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
        >> upload_schema
        >> start_template_job
        >> start_flex_template_job
        >> delete_bucket
    )
    start_flex_template_job >> end
