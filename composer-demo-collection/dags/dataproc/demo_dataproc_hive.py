"""
Example Airflow DAG for DataprocSubmitJobOperator with hive job.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataproc_hive"
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
    "secondary_worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
}
HIVE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
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
    description=' "Hive Job DAG:  Starts by creating a cluster, runs Hive job, then cleans up."  (9 words) \n',
    tags=["demo", "google_cloud", "dataproc", "hive"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
    )
    hive_task = DataprocSubmitJobOperator(
        task_id="hive_task", job=HIVE_JOB, region=LOCATION_REGION, project_id=PROJECT_ID
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=LOCATION_REGION,
    )
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE
    create_cluster >> hive_task >> delete_cluster
