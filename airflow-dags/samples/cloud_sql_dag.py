"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
    CloudSQLCreateInstanceDatabaseOperator
)

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
    "start_date": datetime(2023, 11, 20),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"cloudsql_dag_{VERSION}",
    description="Sample DAG for various Cloud SQL tasks.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    

    
    db_create_body = {"instance": INSTANCE_NAME, "name": DB_NAME, "project": PROJECT_ID}

    sql_db_create_task = CloudSQLCreateInstanceDatabaseOperator(
        body=db_create_body, instance=INSTANCE_NAME, task_id="sql_db_create_task"
    )

    #connection_name ?

    cloud_sql_query_task = CloudSQLExecuteQueryOperator(
        gcp_cloudsql_conn_id=connection_name,
        task_id="cloud_sql_query_task"
        sql="",
        sql_proxy_binary_path=sql_proxy_binary_path,
    )