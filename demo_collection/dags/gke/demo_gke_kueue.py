"""
Example Airflow DAG for Google Kubernetes Engine.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartKueueInsideClusterOperator,
)

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gke_kueue"
LOCATION_REGION = "us-central1"
CLUSTER_NAME = f"cluster-name-test-kueue-{ENV_ID}".replace("_", "-")
CLUSTER = {
    "name": CLUSTER_NAME,
    "initial_node_count": 1,
    "autopilot": {"enabled": True},
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
    description="This Airflow DAG creates a GKE cluster, installs Kueue, and then deletes the cluster.",
    tags=["demo", "google_cloud", "gke"],
) as dag:
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        body=CLUSTER,
    )
    add_kueue_cluster = GKEStartKueueInsideClusterOperator(
        task_id="add_kueue_cluster",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
        kueue_version="v0.5.1",
    )
    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
    )
    create_cluster >> add_kueue_cluster >> delete_cluster
