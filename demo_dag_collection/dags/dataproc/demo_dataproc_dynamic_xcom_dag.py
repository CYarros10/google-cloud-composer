"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
import time

# ----- Dataproc Imports
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.operators.python import PythonOperator


PROJECT_ID = "your-project"
LOCATION_REGION = "us-central1"
CLUSTER_NAME = "your-cluster"
GCS_BUCKET_NAME = "your-bucket"
BIGTABLE_INSTANCE = "your-bigtable-instance"
BIGTABLE_TABLE = "your-bigtable-table"


# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    "demo_dataproc_dynamic_xcom",
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
    description=" This Airflow DAG demonstrates dynamic xcom values via a spark job and sensor.",
    tags=["demo", "google_cloud", "dataproc", "spark", "xcom"],
) as dag:

    def push_value_to_xcom(**kwargs):
        task_instance = kwargs["ti"]
        value_to_push = "cities"
        task_instance.xcom_push(key="my_variable", value=value_to_push)

    # Only create infrastructure if True
    create_cluster = False
    if create_cluster:
        pre_delete_cluster = DataprocDeleteClusterOperator(
            task_id="pre_delete_cluster",
            project_id=PROJECT_ID,
            region=LOCATION_REGION,
            cluster_name=CLUSTER_NAME,
        )

        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=PROJECT_ID,
            region=LOCATION_REGION,
            cluster_name=CLUSTER_NAME,
            cluster_config={
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n1-standard-4",
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": 1024,
                    },
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-4",
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": 1024,
                    },
                },
            },
            trigger_rule="all_done",
        )
        post_delete_cluster = DataprocDeleteClusterOperator(
            task_id="post_delete_cluster",
            project_id=PROJECT_ID,
            region=LOCATION_REGION,
            cluster_name=CLUSTER_NAME,
            trigger_rule="all_done",
        )

    push_task = PythonOperator(
        task_id="push_value",
        python_callable=push_value_to_xcom,
        provide_context=True,
    )

    # create spark jobs for 3 sample files
    for i in range(0, 3):
        # Example of appending to XCOM dynamically
        spark_job_async = DataprocSubmitJobOperator(
            task_id=f"spark_job_async_{i}",
            project_id=PROJECT_ID,
            region=LOCATION_REGION,
            asynchronous=True,
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "spark_job": {
                    "jar_file_uris": [
                        "file:///usr/lib/spark/external/spark-avro.jar",
                        f"gs://{GCS_BUCKET_NAME}/jars/dataproc-templates-1.0-SNAPSHOT.jar",
                    ],
                    "main_class": "com.google.cloud.dataproc.templates.main.DataProcTemplate",  # noqa
                    "args": [
                        "--template",
                        "GCSTOBIGTABLE",
                        "--templateProperty",
                        f"project.id={PROJECT_ID}",
                        "--templateProperty",
                        f"gcs.bigtable.input.location=gs://{GCS_BUCKET_NAME}/{{{{ti.xcom_pull(key='my_variable')}}}}_{i}.csv",
                        "--templateProperty",
                        "gcs.bigtable.input.format=csv",
                        "--templateProperty",
                        f"gcs.bigtable.output.instance.id={BIGTABLE_INSTANCE}",
                        "--templateProperty",
                        f"gcs.bigtable.output.project.id={PROJECT_ID}",
                        "--templateProperty",
                        f"gcs.bigtable.table.name={BIGTABLE_TABLE}",
                        "--templateProperty",
                        "gcs.bigtable.column.family=cf",
                    ],
                },
            },
        )

        # Example of dynamic xcom keys
        spark_job_async_sensor = DataprocJobSensor(
            task_id=f"spark_task_async_sensor_task_{i}",
            project_id=PROJECT_ID,
            region=LOCATION_REGION,
            dataproc_job_id=f"{{{{ ti.xcom_pull('spark_job_async_{i}') }}}}",
        )
        if create_cluster:
            (
                pre_delete_cluster
                >> create_cluster
                >> spark_job_async
                >> spark_job_async_sensor
                >> post_delete_cluster
            )
        else:
            push_task >> spark_job_async >> spark_job_async_sensor
