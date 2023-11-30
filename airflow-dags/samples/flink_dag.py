"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
import time

from airflow.operators.bash import BashOperator

# ----- Dataproc Imports
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor

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
    f"flink_dataproc_dag_{VERSION}",
    description="example flink dataproc dag",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    project_id = "cy-artifacts"
    region = "us-central1"
    dp_cluster_name = "flinky"
    gcs_bucket_name = "cy-sandbox"
    flink_jar_gcs_path = "gs://cy-sandbox/tmp"

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

    # bash operator gcloud
    flink_job_via_gcloud_pig = BashOperator(
        task_id="flink_job_via_gcloud_pig",
        bash_command=f"gcloud dataproc jobs submit pig --cluster=flinky --region=us-central1    -e='fs -cp -f {flink_jar_gcs_path} file:///tmp/the.jar; sh chmod 750 /tmp/the.jar; sh flink run -m yarn-cluster -p 8 -ys 2 -yjm 1024m -ytm 2048m /tmp/the.jar;'",
    )
    
    # pig to execute shell script
    flink_job_via_airflow_pig = DataprocSubmitJobOperator(
        task_id=f"flink_job_via_airflow_pig",
        project_id=project_id,
        region=region,
        asynchronous=False,
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": dp_cluster_name},
            "pig_job": {
                "query_list": {
                    "queries": [
                        f"fs -cp -f {flink_jar_gcs_path} file:///tmp/the.jar; sh chmod 750 /tmp/the.jar; sh flink run -m yarn-cluster -p 8 -ys 2 -yjm 1024m -ytm 2048m /tmp/the.jar;"
                    ]
                }
                   
            },
        }
    )

    # native flink job. not supported? Error: Protocol message Job has no "flink_job" field.
    # flink_job = DataprocSubmitJobOperator(
    #     task_id=f"flink_job",
    #     project_id=project_id,
    #     region=region,
    #     asynchronous=False,
    #     job={
    #         "reference": {"project_id": project_id},
    #         "placement": {"cluster_name": dp_cluster_name},
    #         "flink_job": {
    #             "jar_file_uris": [
    #                 f"gs://{gcs_bucket_name}/tmp",
    #             ]
    #         },
    #     }
    # )

    if create_cluster:
        (
            pre_delete_cluster
            >> create_cluster
            >> [flink_job_via_gcloud_pig, flink_job_via_airflow_pig]
            #>> flink_job
            >> post_delete_cluster
        )
    else:
        flink_job_via_gcloud_pig
        flink_job_via_airflow_pig
        #flink_job
