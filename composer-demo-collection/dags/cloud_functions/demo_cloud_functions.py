"""
Example Airflow DAG that displays interactions with Google Cloud Functions.
It creates a function and then deletes it.
"""

from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionDeleteFunctionOperator,
    CloudFunctionDeployFunctionOperator,
    CloudFunctionInvokeFunctionOperator,
)

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_cloud_function"
LOCATION_REGION = "us-central1"
SHORT_FUNCTION_NAME = "hello_world"
FUNCTION_NAME = (
    f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/functions/{SHORT_FUNCTION_NAME}"
)
SOURCE_ARCHIVE_URL = (
    "gs://airflow-system-tests-resources/cloud-functions/main_function.zip"
)
ENTRYPOINT = "hello_world"
RUNTIME = "python38"
SOURCE_UPLOAD_URL = ""
ZIP_PATH = ""
repo = f"repo-{DAG_ID}-{ENV_ID}".replace("_", "-")
SOURCE_REPOSITORY = ()
VALIDATE_BODY = True
body = {
    "name": FUNCTION_NAME,
    "entryPoint": ENTRYPOINT,
    "runtime": RUNTIME,
    "httpsTrigger": {},
}
default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=55),
}
if SOURCE_ARCHIVE_URL:
    body["sourceArchiveUrl"] = SOURCE_ARCHIVE_URL
elif SOURCE_REPOSITORY:
    body["sourceRepository"] = {"url": SOURCE_REPOSITORY}
elif ZIP_PATH:
    body["sourceUploadUrl"] = ""
    default_args["zip_path"] = ZIP_PATH
elif SOURCE_UPLOAD_URL:
    body["sourceUploadUrl"] = SOURCE_UPLOAD_URL
else:
    raise Exception("Please provide one of the source_code parameters")
with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args=default_args,
    description="This Airflow DAG showcases interaction with Google Cloud Function by creating and invoking it and then deleting it.",
    tags=["demo", "google_cloud", "cloud_functions"],
) as dag:
    deploy_function = CloudFunctionDeployFunctionOperator(
        task_id="deploy_function",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        body=body,
        validate_body=VALIDATE_BODY,
    )
    deploy_function_no_project = CloudFunctionDeployFunctionOperator(
        task_id="deploy_function_no_project",
        location=LOCATION_REGION,
        body=body,
        validate_body=VALIDATE_BODY,
    )
    invoke_function = CloudFunctionInvokeFunctionOperator(
        task_id="invoke_function",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        input_data={},
        function_id=SHORT_FUNCTION_NAME,
    )
    delete_function = CloudFunctionDeleteFunctionOperator(
        task_id="delete_function", name=FUNCTION_NAME
    )
    chain(deploy_function, deploy_function_no_project, invoke_function, delete_function)
