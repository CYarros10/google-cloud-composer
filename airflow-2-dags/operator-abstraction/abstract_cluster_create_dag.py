"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""
from datetime import timedelta, datetime
from airflow import models

from custom_utils import helper_functions

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
)

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_4"

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
    "start_date": datetime(2022, 3, 15),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=1),
}

# -------------------------
# User defined variables
# -------------------------
PROJECT_ID = "cy-artifacts"
REGION = "us-central1"
CLUSTER_NAME = "cluster-from-abstract-task-group-custom-op"

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"custom_operator_dag_{VERSION}",
    description="Sample DAG for custom operator creation.",
    schedule_interval="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    # Airflow Task Group that abstracts cluster configuration and pre-deletion task
    dataproc_cluster_setup =  helper_functions.setup_cluster_taskgroup(
        group_id="dataproc_cluster_setup",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        idle_delete="86400s" # 1 day
    )

    forever_dataproc_cluster_setup =  helper_functions.setup_cluster_taskgroup(
        group_id="forever_dataproc_cluster_setup",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="forever-dataproc",
    )

    dataproc_cluster_deletion = DataprocDeleteClusterOperator(
        task_id="post_delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",
    )

    dataproc_cluster_setup >> dataproc_cluster_deletion