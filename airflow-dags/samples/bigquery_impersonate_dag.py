"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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
    "start_date": datetime(2023, 8, 1),
    "sla": timedelta(minutes=25),
    "deferrable": False,
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"bigquery_impersonation_dag_{VERSION}",
    description="Sample DAG that uses a service account impersonation.",
    schedule="0 0 * * *",  # daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    info_schema_job = BigQueryInsertJobOperator(
        task_id="info_schema_job",
        job_id="info_schema_job",
        configuration={
            "query": {
                "query": """
                    SELECT 
                        * 
                    FROM 
                        `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
                    LIMIT 10;
                """,
                "useLegacySql": False,
            },
            "labels": {
                "costcenter": "12345",
            },
        },
        location="US",
        impersonation_chain="",
    )
