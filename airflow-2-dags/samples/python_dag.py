"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from airflow import models
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
    dag_id=f"python_dag_{VERSION}",
    description="Sample DAG for a python task.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
):

    def my_args(*args, **kwargs):
        print("My Args are {}".format(args))
        print("My Kwargs are {}".format(kwargs))

    python_task = PythonOperator(
        task_id="python-task",
        python_callable=my_args,
        op_args={"a", "b", "c"},
        op_kwargs={"a": "2"},
    )
