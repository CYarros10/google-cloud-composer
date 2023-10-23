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
    source_blob_name="configs/gcs_config.yaml"
)
MAX_ACTIVE_RUNS=config.get('max_active_runs', 2)
ECHO_COMMAND=config.get('echo_cmd', "echo 'hello default'")

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
}

user_defined_macros = {}

with models.DAG(
    dag_id=f"bash_echo_conf_dag_{VERSION}",
    description="Sample DAG with a configurable bash operator.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    dagrun_timeout=timedelta(minutes=30),
):
    # Here's a task based on Bash Operator!

    bash_task = BashOperator(
        task_id="bash_task_1",
        bash_command=ECHO_COMMAND,
    )