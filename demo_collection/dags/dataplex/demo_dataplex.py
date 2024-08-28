"""
Example Airflow DAG that shows how to use Dataplex.
"""

from __future__ import annotations
import datetime
from datetime import timedelta
import os
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateLakeOperator,
    DataplexCreateTaskOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteTaskOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.sensors.dataplex import DataplexTaskStateSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_dataplex"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
SPARK_FILE_NAME = "spark_example_pi.py"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
LAKE_ID = f"test-lake-dataplex-{ENV_ID}"
LOCATION_REGION = "us-central1"
SERVICE_ACC = f"{PROJECT_ID}@appspot.gserviceaccount.com"
SPARK_FILE_FULL_PATH = f"gs://{BUCKET_NAME}/{SPARK_FILE_NAME}"
DATAPLEX_TASK_ID = f"test-task-{ENV_ID}"
TRIGGER_SPEC_TYPE = "ON_DEMAND"
EXAMPLE_TASK_BODY = {
    "trigger_spec": {"type_": TRIGGER_SPEC_TYPE},
    "execution_spec": {"service_account": SERVICE_ACC},
    "spark": {"python_script_file": SPARK_FILE_FULL_PATH},
}
EXAMPLE_LAKE_BODY = {
    "display_name": "test_display_name",
    "labels": [],
    "description": "test_description",
    "metastore": {"service": ""},
}
with DAG(
    DAG_ID,
    start_date=datetime.datetime(2024, 1, 1),
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
    description="This DAG creates a dataplex lake, triggers multiple airflow tasks that execute a data operation, then deletes the dataplex lake.",
    tags=["demo", "google_cloud", "dataplex"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    sync_bucket = GCSSynchronizeBucketsOperator(
        task_id="sync_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object=SPARK_FILE_NAME,
        destination_bucket=BUCKET_NAME,
        destination_object=SPARK_FILE_NAME,
        recursive=True,
    )
    create_lake = DataplexCreateLakeOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        body=EXAMPLE_LAKE_BODY,
        lake_id=LAKE_ID,
        task_id="create_lake",
    )
    create_dataplex_task = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="create_dataplex_task",
    )
    create_dataplex_task_async = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=f"{DATAPLEX_TASK_ID}-1",
        asynchronous=True,
        task_id="create_dataplex_task_async",
    )
    delete_dataplex_task_async = DataplexDeleteTaskOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=f"{DATAPLEX_TASK_ID}-1",
        task_id="delete_dataplex_task_async",
    )
    list_dataplex_task = DataplexListTasksOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        task_id="list_dataplex_task",
    )
    get_dataplex_task = DataplexGetTaskOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="get_dataplex_task",
    )
    dataplex_task_state = DataplexTaskStateSensor(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="dataplex_task_state",
    )
    delete_dataplex_task = DataplexDeleteTaskOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="delete_dataplex_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_lake = DataplexDeleteLakeOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        task_id="delete_lake",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    chain(
        create_bucket,
        sync_bucket,
        create_lake,
        create_dataplex_task,
        get_dataplex_task,
        list_dataplex_task,
        create_dataplex_task_async,
        delete_dataplex_task_async,
        dataplex_task_state,
        delete_dataplex_task,
        delete_lake,
        delete_bucket,
    )
