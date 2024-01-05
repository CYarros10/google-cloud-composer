"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
    DataprocListBatchesOperator,
)

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

# -------------------------
# Tags, Default Args, and Macros
# -------------------------
tags = ["application:samples"]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 8, 17),
    "project_id": "",
    "region": "us-central1",
    "mode": "reschedule",  # default sensor mode
    "poke_interval": 60,
    "batch_id": "create-spark-batch-via-aiflow",
    "sla": timedelta(minutes=25),
}

user_defined_macros = {}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"dataproc_serverless_dag_{VERSION}",
    description="Sample DAG for various Dataproc Serverless tasks.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    create_spark_batch = DataprocCreateBatchOperator(
        task_id="create_spark_batch",
        batch={
            "spark_batch": {
                "main_class": "org.apache.spark.examples.SparkPi",
                "jar_file_uris": [
                    "file:///usr/lib/spark/examples/jars/spark-examples.jar"
                ],
                "args": ["1000"],
            }
        },
        asynchronous=True,
    )

    # requires apache-airflow-providers-google >= 8.9.0
    # batch_async_sensor = DataprocBatchSensor(
    #     task_id="batch_async_sensor",
    #     timeout= 60 * 5
    # )

    list_batches = DataprocListBatchesOperator(
        task_id="list-all-batches", region="us-central1"
    )

    get_batch = DataprocGetBatchOperator(
        task_id="get_batch",
        region="{{region}}",
    )
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch",
        region="{{region}}",
    )
    create_spark_batch >> list_batches >> get_batch >> delete_batch
