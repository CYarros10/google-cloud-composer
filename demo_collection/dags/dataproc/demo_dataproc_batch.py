"""
Example Airflow DAG for Dataproc batch operators.
"""

from datetime import datetime, timedelta
from google.api_core.retry_async import AsyncRetry
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCancelOperationOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
    DataprocListBatchesOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_dataproc_batch"
LOCATION_REGION = "us-central1"
BATCH_ID = f"batch-{ENV_ID}-{DAG_ID}".replace("_", "-")
BATCH_ID_2 = f"batch-{ENV_ID}-{DAG_ID}-2".replace("_", "-")
BATCH_ID_3 = f"batch-{ENV_ID}-{DAG_ID}-3".replace("_", "-")
BATCH_ID_4 = f"batch-{ENV_ID}-{DAG_ID}-4".replace("_", "-")
BATCH_CONFIG = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
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
    },
    description="This DAG creates and deletes Dataproc batches for Spark jobs with retries and async operations.",
    tags=["demo", "google_cloud", "dataproc", "batch", "spark"],
) as dag:
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
    )
    create_batch_2 = DataprocCreateBatchOperator(
        task_id="create_batch_2",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_2,
        result_retry=AsyncRetry(maximum=10.0, initial=10.0, multiplier=1.0),
    )
    create_batch_3 = DataprocCreateBatchOperator(
        task_id="create_batch_3",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_3,
        asynchronous=True,
    )
    batch_async_sensor = DataprocBatchSensor(
        task_id="batch_async_sensor",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        batch_id=BATCH_ID_3,
        poke_interval=10,
    )
    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region=LOCATION_REGION, batch_id=BATCH_ID
    )
    get_batch_2 = DataprocGetBatchOperator(
        task_id="get_batch_2", project_id=PROJECT_ID, region=LOCATION_REGION, batch_id=BATCH_ID_2
    )
    list_batches = DataprocListBatchesOperator(
        task_id="list_batches", project_id=PROJECT_ID, region=LOCATION_REGION
    )
    create_batch_4 = DataprocCreateBatchOperator(
        task_id="create_batch_4",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID_4,
        asynchronous=True,
    )
    cancel_operation = DataprocCancelOperationOperator(
        task_id="cancel_operation",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        operation_name="{{ task_instance.xcom_pull('create_batch_4') }}",
    )
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch", project_id=PROJECT_ID, region=LOCATION_REGION, batch_id=BATCH_ID
    )
    delete_batch_2 = DataprocDeleteBatchOperator(
        task_id="delete_batch_2",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch_id=BATCH_ID_2,
    )
    delete_batch_3 = DataprocDeleteBatchOperator(
        task_id="delete_batch_3",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch_id=BATCH_ID_3,
    )
    delete_batch_4 = DataprocDeleteBatchOperator(
        task_id="delete_batch_4",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch_id=BATCH_ID_4,
    )
    delete_batch.trigger_rule = TriggerRule.ALL_DONE
    delete_batch_2.trigger_rule = TriggerRule.ALL_DONE
    delete_batch_3.trigger_rule = TriggerRule.ALL_DONE
    delete_batch_4.trigger_rule = TriggerRule.ALL_DONE
    (
        [create_batch, create_batch_2, create_batch_3]
        >> batch_async_sensor
        >> [get_batch, get_batch_2, list_batches]
        >> create_batch_4
        >> cancel_operation
        >> [delete_batch, delete_batch_2, delete_batch_3, delete_batch_4]
    )
