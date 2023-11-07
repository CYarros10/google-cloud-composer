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
    dag_id=f"basic_xcom_dag_{VERSION}",
    description="Sample DAG for a python xcom task.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
):

    def push_value_to_xcom(**kwargs):
        task_instance = kwargs["ti"]
        value_to_push = "Hello, World!"
        task_instance.xcom_push(key="my_variable", value=value_to_push)

    def pull_value_from_xcom(**kwargs):
        task_instance = kwargs["ti"]
        value_pulled = task_instance.xcom_pull(key="my_variable")
        print(f"Pulled value: {value_pulled}")

    # Define two PythonOperators
    push_task = PythonOperator(
        task_id="push_value",
        python_callable=push_value_to_xcom,
        provide_context=True,
    )

    pull_task = PythonOperator(
        task_id="pull_value",
        python_callable=pull_value_from_xcom,
        provide_context=True,
    )

    # Set the task dependencies
    push_task >> pull_task
