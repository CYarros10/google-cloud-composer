"""
Example Airflow DAG for DataprocSubmitJobOperator with async spark job.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataproc_spark_async"
CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}".replace("_", "-")
LOCATION_REGION = "us-central1"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
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
    description="This Airflow DAG creates a Dataproc cluster, submits an asynchronous Spark job, waits for it to finish with a sensor, and finally deletes the cluster. ",
    tags=["demo", "google_cloud", "dataproc", "spark"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
    )
    spark_task_async = DataprocSubmitJobOperator(
        task_id="spark_task_async",
        job=SPARK_JOB,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        asynchronous=True,
    )
    spark_task_async_sensor = DataprocJobSensor(
        task_id="spark_task_async_sensor_task",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        dataproc_job_id=spark_task_async.output,
        poke_interval=10,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_cluster >> spark_task_async >> spark_task_async_sensor >> delete_cluster
