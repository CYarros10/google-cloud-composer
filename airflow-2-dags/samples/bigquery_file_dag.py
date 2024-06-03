"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""
from datetime import timedelta, datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

VERSION = "v0_0_0"
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
with models.DAG(
    f"bigquery_file_dag_{VERSION}",
    description="Sample DAG for BigQuery operations.",
    schedule_interval="0 0 * * *",
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    bq_insert_job = BigQueryInsertJobOperator(
        task_id="bq_insert_job",
        configuration={
            "query": {"query": "{% include 'sql/test.sql' %}", "useLegacySql": False}
        },
        location="US",
    )
