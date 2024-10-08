"""
Example Airflow DAG that shows how to use DataFusion.
"""

from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.datafusion import DataFusionHook
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionCreateInstanceOperator,
    CloudDataFusionCreatePipelineOperator,
    CloudDataFusionDeleteInstanceOperator,
    CloudDataFusionDeletePipelineOperator,
    CloudDataFusionGetInstanceOperator,
    CloudDataFusionListPipelinesOperator,
    CloudDataFusionRestartInstanceOperator,
    CloudDataFusionStartPipelineOperator,
    CloudDataFusionStopPipelineOperator,
    CloudDataFusionUpdateInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.sensors.datafusion import (
    CloudDataFusionPipelineStateSensor,
)
from airflow.utils.trigger_rule import TriggerRule

SERVICE_ACCOUNT = "488712714114-compute@developer.gserviceaccount.com"
ENV_ID = "composer"
PROJECT_ID = "your-project"
LOCATION_REGION = "europe-north1"
DAG_ID = "demo_datafusion"
INSTANCE_NAME = f"df-{ENV_ID}".replace("_", "-")
INSTANCE = {
    "type": "BASIC",
    "displayName": INSTANCE_NAME,
    "dataprocServiceAccount": SERVICE_ACCOUNT,
}
BUCKET_NAME_1 = f"bucket1-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_NAME_2 = f"bucket2-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_NAME_1_URI = f"gs://{BUCKET_NAME_1}"
BUCKET_NAME_2_URI = f"gs://{BUCKET_NAME_2}"
PIPELINE_NAME = f"pipe-{ENV_ID}".replace("_", "-")
PIPELINE = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "{{ task_instance.xcom_pull(task_ids='get_artifacts_versions')['cdap-data-pipeline'] }}",
        "scope": "SYSTEM",
    },
    "description": "Data Pipeline Application",
    "name": PIPELINE_NAME,
    "config": {
        "resources": {"memoryMB": 2048, "virtualCores": 1},
        "driverResources": {"memoryMB": 2048, "virtualCores": 1},
        "connections": [{"from": "GCS", "to": "GCS2"}],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCSFile",
                    "type": "batchsource",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "{{ task_instance.xcom_pull(task_ids='get_artifacts_versions')                            ['google-cloud'] }}",
                        "scope": "SYSTEM",
                    },
                    "properties": {
                        "project": "auto-detect",
                        "format": "text",
                        "skipHeader": "false",
                        "serviceFilePath": "auto-detect",
                        "filenameOnly": "false",
                        "recursive": "false",
                        "encrypted": "false",
                        "schema": '{"type":"record","name":"textfile","fields":[{"name"                            :"offset","type":"long"},{"name":"body","type":"string"}]}',
                        "path": BUCKET_NAME_1_URI,
                        "referenceName": "foo_bucket",
                        "useConnection": "false",
                        "serviceAccountType": "filePath",
                        "sampleSize": "1000",
                        "fileEncoding": "UTF-8",
                    },
                },
                "outputSchema": '{"type":"record","name":"textfile","fields"                    :[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                "id": "GCS",
            },
            {
                "name": "GCS2",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS2",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "{{ task_instance.xcom_pull(task_ids='get_artifacts_versions')                            ['google-cloud'] }}",
                        "scope": "SYSTEM",
                    },
                    "properties": {
                        "project": "auto-detect",
                        "suffix": "yyyy-MM-dd-HH-mm",
                        "format": "json",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "schema": '{"type":"record","name":"textfile","fields":[{"name"                            :"offset","type":"long"},{"name":"body","type":"string"}]}',
                        "referenceName": "bar",
                        "path": BUCKET_NAME_2_URI,
                        "serviceAccountType": "filePath",
                        "contentType": "application/octet-stream",
                    },
                },
                "outputSchema": '{"type":"record","name":"textfile","fields"                    :[{"name":"offset","type":"long"},{"name":"body","type":"string"}]}',
                "inputSchema": [
                    {
                        "name": "GCS",
                        "schema": '{"type":"record","name":"textfile","fields":[{"name"                            :"offset","type":"long"},{"name":"body","type":"string"}]}',
                    }
                ],
                "id": "GCS2",
            },
        ],
        "schedule": "0 * * * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "description": "Data Pipeline Application",
        "maxConcurrentRuns": 1,
    },
}
CloudDataFusionCreatePipelineOperator.template_fields = (
    *CloudDataFusionCreatePipelineOperator.template_fields,
    "pipeline",
)
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
    },
    description="The DAG deploys a Data Fusion instance and runs a Spark pipeline. It creates buckets, creates the instance, gets the instance, restarts the instance, updates the instance, creates a pipeline, lists pipelines, starts a pipeline, stops a pipeline, deletes the pipeline, deletes the instance, and finally deletes the buckets.",
    tags=["demo", "google_cloud", "datafusion"],
) as dag:
    create_bucket1 = GCSCreateBucketOperator(
        task_id="create_bucket1", bucket_name=BUCKET_NAME_1, project_id=PROJECT_ID
    )
    create_bucket2 = GCSCreateBucketOperator(
        task_id="create_bucket2", bucket_name=BUCKET_NAME_2, project_id=PROJECT_ID
    )
    create_instance = CloudDataFusionCreateInstanceOperator(
        location=LOCATION_REGION,
        instance_name=INSTANCE_NAME,
        instance=INSTANCE,
        task_id="create_instance",
    )
    get_instance = CloudDataFusionGetInstanceOperator(
        location=LOCATION_REGION, instance_name=INSTANCE_NAME, task_id="get_instance"
    )
    restart_instance = CloudDataFusionRestartInstanceOperator(
        location=LOCATION_REGION, instance_name=INSTANCE_NAME, task_id="restart_instance"
    )
    update_instance = CloudDataFusionUpdateInstanceOperator(
        location=LOCATION_REGION,
        instance_name=INSTANCE_NAME,
        instance=INSTANCE,
        update_mask="",
        task_id="update_instance",
    )

    @task(task_id="get_artifacts_versions")
    def get_artifacts_versions(ti=None):
        hook = DataFusionHook()
        instance_url = ti.xcom_pull(task_ids="get_instance", key="return_value")[
            "apiEndpoint"
        ]
        artifacts = hook.get_instance_artifacts(
            instance_url=instance_url, namespace="default"
        )
        return {item["name"]: item["version"] for item in artifacts}

    create_pipeline = CloudDataFusionCreatePipelineOperator(
        location=LOCATION_REGION,
        pipeline_name=PIPELINE_NAME,
        pipeline=PIPELINE,
        instance_name=INSTANCE_NAME,
        task_id="create_pipeline",
    )
    list_pipelines = CloudDataFusionListPipelinesOperator(
        location=LOCATION_REGION, instance_name=INSTANCE_NAME, task_id="list_pipelines"
    )
    start_pipeline = CloudDataFusionStartPipelineOperator(
        location=LOCATION_REGION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        task_id="start_pipeline",
    )
    start_pipeline_sensor = CloudDataFusionPipelineStateSensor(
        task_id="pipeline_state_sensor",
        pipeline_name=PIPELINE_NAME,
        pipeline_id=start_pipeline.output,
        expected_statuses=["COMPLETED"],
        failure_statuses=["FAILED"],
        instance_name=INSTANCE_NAME,
        location=LOCATION_REGION,
    )
    stop_pipeline = CloudDataFusionStopPipelineOperator(
        location=LOCATION_REGION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        task_id="stop_pipeline",
    )
    delete_pipeline = CloudDataFusionDeletePipelineOperator(
        location=LOCATION_REGION,
        pipeline_name=PIPELINE_NAME,
        instance_name=INSTANCE_NAME,
        task_id="delete_pipeline",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_instance = CloudDataFusionDeleteInstanceOperator(
        location=LOCATION_REGION,
        instance_name=INSTANCE_NAME,
        task_id="delete_instance",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket1 = GCSDeleteBucketOperator(
        task_id="delete_bucket1",
        bucket_name=BUCKET_NAME_1,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket2 = GCSDeleteBucketOperator(
        task_id="delete_bucket2",
        bucket_name=BUCKET_NAME_1,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket1, create_bucket2]
        >> create_instance
        >> get_instance
        >> get_artifacts_versions()
        >> restart_instance
        >> update_instance
        >> create_pipeline
        >> list_pipelines
        >> start_pipeline
        >> start_pipeline_sensor
        >> stop_pipeline
        >> delete_pipeline
        >> delete_instance
        >> [delete_bucket1, delete_bucket2]
    )
