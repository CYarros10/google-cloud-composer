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
    f"spark_to_bigtable_demo_v0",
    description="example spark to bigtable dag",
    schedule="0 0 * * *",  # midnight daily
    tags=["type:demo"],
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    create_cluster=False
    project_id = "cy-artifacts"
    cluster="pyspark-to-bigtable-test-cluster"
    region="us-central1"
    bigtable_instance="bt-test-instance"
    bigtable_table="test"

    spark_job = DataprocSubmitJobOperator(
        task_id="spark_job",
        project_id=project_id,
        region=region,
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": "pyspark-to-bt-test"},
            "spark_job": {
                "jar_file_uris": ["gs://airflow-reporting-cy/java/bigtable-spark-example-0.0.1-SNAPSHOT.jar"],
                "main_class": "bigtable.spark.example.WordCount",
                "args": [
                    project_id,
                    bigtable_instance,
                    "sparkcounttable"
                ] 
            },
        },
    )    

    pyspark_job = DataprocSubmitJobOperator(
        task_id="pyspark_job",
        project_id=project_id,
        region=region,
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": "pyspark-to-bt-test"},
            "pyspark_job": {
                "main_python_file_uri": "gs://airflow-reporting-cy/pyspark/word_count.py",
                "jar_file_uris": [
                    "gs://bigtable-spark-preview/jars/bigtable-spark-0.0.1-preview1-SNAPSHOT.jar"
                ],
                "args": [
                    "--bigtableProjectId",
                    project_id,
                    "--bigtableInstanceId",
                    bigtable_instance,
                    "--bigtableTableName",
                    "pysparkcounttable"
                ]           
            },
        },
    )

    if create_cluster:
        pre_delete_cluster = DataprocDeleteClusterOperator(
            task_id="pre_delete_cluster",
            project_id=project_id,
            cluster_name=cluster,
            region=region,
        )

        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=project_id,
            cluster_name=cluster,
            region=region,
            cluster_config={
                "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n1-standard-4",
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": 500,
                    },
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-4",
                    "disk_config": {
                        "boot_disk_type": "pd-standard",
                        "boot_disk_size_gb": 500,
                    },
                },
            },
            trigger_rule="all_done",
        )
        post_delete_cluster = DataprocDeleteClusterOperator(
            task_id="post_delete_cluster",
            project_id=project_id,
            cluster_name=cluster,
            region=region,
            trigger_rule="all_done",
        )
    
        pre_delete_cluster >> create_cluster >> [spark_job, pyspark_job] >> post_delete_cluster
