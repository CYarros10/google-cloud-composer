"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import datetime, timedelta

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

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
    dag_id=f"params_dag_{VERSION}",
    description="Sample DAG for a parameterized python task.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
    params={
        "x": Param(5, type="integer", minimum=3),
        "my_int_param": 6
    },
):

    def multiply_by_100(*args, **kwargs):
        my_int_param = args[0]
        return my_int_param * 100

    param_python_task = PythonOperator(
        task_id="param_python_task",
        op_args=[
            "{{ params.my_int_param }}",
        ],
        python_callable=multiply_by_100
    )
    
