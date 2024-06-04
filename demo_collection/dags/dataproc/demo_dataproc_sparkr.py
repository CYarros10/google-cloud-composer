"""
Example Airflow DAG for DataprocSubmitJobOperator with sparkr job.
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
DAG_ID = "demo_dataproc_sparkr"
JOB_FILE_URI = (
    "gs://airflow-system-tests-resources/dataproc/sparkr/dataproc-sparkr-job.r"
)
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
SPARKR_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_r_job": {"main_r_file_uri": JOB_FILE_URI},
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
    description="This Airflow DAG uses DataprocSubmitJobOperator with Dataproc cluster to run a SparkR job.",
    tags=["demo", "google_cloud", "dataproc", "spark_r"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
    )
    sparkr_task = DataprocSubmitJobOperator(
        task_id="sparkr_task", job=SPARKR_JOB, region=LOCATION_REGION, project_id=PROJECT_ID
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_cluster >> sparkr_task >> delete_cluster
