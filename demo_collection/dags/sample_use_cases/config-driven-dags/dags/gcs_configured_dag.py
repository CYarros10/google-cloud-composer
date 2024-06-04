"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from airflow import models
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.tooling import load_config_from_gcs

config = load_config_from_gcs(
    bucket_name="",
    source_blob_name="gcs_dag_configs/gcs.yaml"
)

# -------------------------
# Default Args, and Macros
# -------------------------

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2023, 8, 1),
    "sla": timedelta(minutes=config.get('SLA_MINS', 20)),
    "retries": config.get('RETRIES', 1),
    "retry_delay": timedelta(minutes=2),
}

user_defined_macros = {}

with models.DAG(
    dag_id=f"gcs_config_v0_2",
    description="Sample DAG with a configurable bash operator.",
    schedule="0 0 * * *",  # midnight daily
    tags=config.get('TAGS', []),
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=config.get('MAX_ACTIVE_RUNS', 2),
    max_active_tasks=config.get('MAX_ACTIVE_TASKS', 16),
    dagrun_timeout=timedelta(minutes=config.get('TIMEOUT_MINS', 30)),
):
    # Here's a task based on Bash Operator!

    bash_task = BashOperator(
        task_id="bash_task_1",
        bash_command="echo 'hello'",
    )
