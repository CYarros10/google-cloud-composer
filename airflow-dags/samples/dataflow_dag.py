"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
from airflow.utils.task_group import TaskGroup

# ------ Dataflow Airflow Imports
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
    DataflowStopJobOperator,
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
    "start_date": datetime(2022, 3, 15),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}
user_defined_macros = {
    "project_id": "cy-artifacts",
    "region": "us-central1",
    "df_job": "health-check-dataflow-job",
    "df_python_script": "wordcount.py",
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"health_check_{VERSION}",
    description="example health check DAG",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    # -------------------------------------------------------------------------
    # Dataflow
    # -------------------------------------------------------------------------

    with TaskGroup(group_id="dataflow_tg1") as dataflow_tg1:
        start_template_job = DataflowTemplatedJobStartOperator(
            task_id="start_template_job",
            job_name="{{df_job}}-template",
            project_id="{{ project_id }}",
            template="gs://dataflow-templates/latest/Word_Count",
            parameters={
                "inputFile": "gs://{{gcs_download_bucket}}/{{gcs_download_obj}}",
                "output": "gs://{{gcs_download_bucket}}/dataflow_output",
            },
            location="{{region}}",
        )

        stop_template_job = DataflowStopJobOperator(
            task_id="stop_dataflow_job",
            location="{{region}}",
            job_name_prefix="{{df_job}}-template",
        )

        start_template_job >> stop_template_job
