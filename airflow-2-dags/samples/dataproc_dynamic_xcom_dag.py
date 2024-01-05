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

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_5"

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
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}
timestr = time.strftime("%Y%m%d-%H%M%S")

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"dataproc_dynamic_xcom_{VERSION}",
    description="example dataproc dag",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    project_id = "your-project"
    region = "us-central1"
    dp_cluster_name = "your-cluster"
    gcs_bucket_name = "your-bucket"
    bigtable_instance = "your-bigtable-instance"
    bigtable_table = "your-bigtable-table"

    def push_value_to_xcom(**kwargs):
        task_instance = kwargs["ti"]
        value_to_push = "cities"
        task_instance.xcom_push(key="my_variable", value=value_to_push)

    # Only create infrastructure if True
    create_cluster = False
    if create_cluster:
        pre_delete_cluster = DataprocDeleteClusterOperator(
            task_id="pre_delete_cluster",
            project_id=project_id,
            region=region,
            cluster_name=dp_cluster_name,
        )

        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=project_id,
            region=region,
            cluster_name=dp_cluster_name,
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
            project_id=project_id,
            region=region,
            cluster_name=dp_cluster_name,
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
            project_id=project_id,
            region=region,
            asynchronous=True,
            job={
                "reference": {"project_id": project_id},
                "placement": {"cluster_name": dp_cluster_name},
                "spark_job": {
                    "jar_file_uris": [
                        "file:///usr/lib/spark/external/spark-avro.jar",
                        f"gs://{gcs_bucket_name}/jars/dataproc-templates-1.0-SNAPSHOT.jar",
                    ],
                    "main_class": "com.google.cloud.dataproc.templates.main.DataProcTemplate",  # noqa
                    "args": [
                        "--template",
                        "GCSTOBIGTABLE",
                        "--templateProperty",
                        f"project.id={project_id}",
                        "--templateProperty",
                        f"gcs.bigtable.input.location=gs://{gcs_bucket_name}/{{{{ti.xcom_pull(key='my_variable')}}}}_{i}.csv",
                        "--templateProperty",
                        "gcs.bigtable.input.format=csv",
                        "--templateProperty",
                        f"gcs.bigtable.output.instance.id={bigtable_instance}",
                        "--templateProperty",
                        f"gcs.bigtable.output.project.id={project_id}",
                        "--templateProperty",
                        f"gcs.bigtable.table.name={bigtable_table}",
                        "--templateProperty",
                        "gcs.bigtable.column.family=cf",
                    ],
                },
            },
        )

        # Example of dynamic xcom keys
        spark_job_async_sensor = DataprocJobSensor(
            task_id=f"spark_task_async_sensor_task_{i}",
            project_id=project_id,
            region=region,
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
