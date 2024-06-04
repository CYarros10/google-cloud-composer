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

timestr = time.strftime("%Y%m%d-%H%M%S")

# -------------------------
# Begin DAG Generation
# -------------------------

PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"
CLUSTER_NAME = "demo-flink-cluster"
GCS_BUCKET_NAME = "your-bucket-name"
FLINK_JAR_GCS_PATH = ""

with models.DAG(
    "demo_flink_dataproc",
    schedule="@once",  # midnight daily
    default_args={
        "owner": "Google",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "start_date": datetime(2024, 1, 1),
        "mode": "reschedule",
        "poke_interval": 60,
        "sla": timedelta(minutes=25),
    },
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
    description="This Airflow DAG runs a Flink job on a Dataproc cluster.",
    tags=["demo", "dataproc", "flink"]
) as dag:

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

    # bash operator gcloud
    flink_job_via_gcloud_pig = BashOperator(
        task_id="flink_job_via_gcloud_pig",
        bash_command=f"gcloud dataproc jobs submit pig --cluster=flinky --region=us-central1    -e='fs -cp -f {FLINK_JAR_GCS_PATH} file:///tmp/the.jar; sh chmod 750 /tmp/the.jar; sh flink run -m yarn-cluster -p 8 -ys 2 -yjm 1024m -ytm 2048m /tmp/the.jar;'",
    )
    
    # pig to execute shell script
    flink_job_via_airflow_pig = DataprocSubmitJobOperator(
        task_id=f"flink_job_via_airflow_pig",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        asynchronous=False,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pig_job": {
                "query_list": {
                    "queries": [
                        f"fs -cp -f {FLINK_JAR_GCS_PATH} file:///tmp/the.jar; sh chmod 750 /tmp/the.jar; sh flink run -m yarn-cluster -p 8 -ys 2 -yjm 1024m -ytm 2048m /tmp/the.jar;"
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
