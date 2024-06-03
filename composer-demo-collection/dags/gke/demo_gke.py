"""
Example Airflow DAG for Google Kubernetes Engine.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gke"
LOCATION_REGION = "us-central1"
ZONE = f"{LOCATION_REGION}-a"
CLUSTER_NAME = f"cluster-name-test-build-{ENV_ID}"
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}
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
    description="This Airflow DAG creates a GKE cluster, runs a pod, and then deletes the cluster.",
    tags=["demo", "google_cloud", "gke"],
) as dag:
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster", project_id=PROJECT_ID, location=ZONE, body=CLUSTER
    )
    pod_task = GKEStartPodOperator(
        task_id="pod_task",
        project_id=PROJECT_ID,
        location=ZONE,
        cluster_name=CLUSTER_NAME,
        namespace="default",
        image="perl",
        name="test-pod",
        in_cluster=False,
        on_finish_action="delete_pod",
    )
    pod_task_xcom = GKEStartPodOperator(
        task_id="pod_task_xcom",
        project_id=PROJECT_ID,
        location=ZONE,
        cluster_name=CLUSTER_NAME,
        do_xcom_push=True,
        namespace="default",
        image="alpine",
        cmds=[
            "sh",
            "-c",
            "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json",
        ],
        name="test-pod-xcom",
        in_cluster=False,
        on_finish_action="delete_pod",
    )
    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('pod_task_xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )
    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=ZONE,
    )
    create_cluster >> pod_task >> delete_cluster
    create_cluster >> pod_task_xcom >> delete_cluster
    pod_task_xcom >> pod_task_xcom_result
