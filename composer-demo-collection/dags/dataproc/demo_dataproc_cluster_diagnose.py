"""
Example Airflow DAG for DataprocDiagnoseClusterOperator.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocDiagnoseClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataproc_diagnose_cluster"
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
    description="Analyze cluster health and system issues.",
    tags=["demo", "google_cloud", "dataproc"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
    )
    diagnose_cluster = DataprocDiagnoseClusterOperator(
        task_id="diagnose_cluster",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
    )
    diagnose_cluster_deferrable = DataprocDiagnoseClusterOperator(
        task_id="diagnose_cluster_deferrable",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        deferrable=True,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_cluster >> diagnose_cluster >> delete_cluster
