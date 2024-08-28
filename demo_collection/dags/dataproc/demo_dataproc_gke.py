"""
Example Airflow DAG that show how to create a Dataproc cluster in Google Kubernetes Engine.

Required environment variables:
A GKE cluster can support multiple DP clusters running in different namespaces.
Define a namespace or assign a default one.

"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_dataproc_gke"
LOCATION_REGION = "us-central1"
CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}".replace("_", "-")
GKE_CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}-gke".replace("_", "-")
WORKLOAD_POOL = f"{PROJECT_ID}.svc.id.goog"
GKE_CLUSTER_CONFIG = {
    "name": GKE_CLUSTER_NAME,
    "workload_identity_config": {"workload_pool": WORKLOAD_POOL},
    "initial_node_count": 1,
}
VIRTUAL_CLUSTER_CONFIG = {
    "kubernetes_cluster_config": {
        "gke_cluster_config": {
            "gke_cluster_target": f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/clusters/{GKE_CLUSTER_NAME}",
            "node_pool_target": [
                {
                    "node_pool": f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/clusters/{GKE_CLUSTER_NAME}/nodePools/default-pool",
                    "roles": ["DEFAULT"],
                    "node_pool_config": {"config": {"preemptible": True}},
                }
            ],
        },
        "kubernetes_software_config": {"component_version": {"SPARK": b"3"}},
    },
    "staging_bucket": "test-staging-bucket",
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
    description="This Airflow DAG creates a Dataproc cluster in a GKE cluster, running the Spark workload.",
    tags=["demo", "google_cloud", "dataproc", "gke", "spark"],
) as dag:
    create_gke_cluster = GKECreateClusterOperator(
        task_id="create_gke_cluster",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        body=GKE_CLUSTER_CONFIG,
    )
    create_cluster_in_gke = DataprocCreateClusterOperator(
        task_id="create_cluster_in_gke",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
        virtual_cluster_config=VIRTUAL_CLUSTER_CONFIG,
    )
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_gke_cluster = GKEDeleteClusterOperator(
        task_id="delete_gke_cluster",
        name=GKE_CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_gke_cluster
        >> create_cluster_in_gke
        >> delete_dataproc_cluster
        >> delete_gke_cluster
    )
