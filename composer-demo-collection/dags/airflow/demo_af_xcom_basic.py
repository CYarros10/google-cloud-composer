"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from airflow import models
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with models.DAG(
    dag_id=f"demo_af_xcom_basic",
    description="Sample DAG for a python xcom task.",
    schedule="@once",
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Google",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2024, 1, 1),
        "sla": timedelta(minutes=55),
    },
    tags=['airflow', 'demo', 'xcom', 'python'],
):

    def push_value_to_xcom(**kwargs):
        task_instance = kwargs["ti"]
        value_to_push = "Hello, World!"
        task_instance.xcom_push(key="my_variable", value=value_to_push)

    def pull_value_from_xcom(**kwargs):
        task_instance = kwargs["ti"]
        value_pulled = task_instance.xcom_pull(key="my_variable")
        print(f"Pulled value: {value_pulled}")

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

    push_task >> pull_task
