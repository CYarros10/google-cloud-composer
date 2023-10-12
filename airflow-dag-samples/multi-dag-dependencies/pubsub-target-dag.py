from datetime import datetime, timedelta
from airflow import models
import time

from airflow.operators.python import PythonOperator

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_1"
PROJECT = ""

#-------------------------
# Tags, Default Args, and Macros
#-------------------------
tags = [
    f"project:{PROJECT}",
]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023,8,1),
    "sla": timedelta(minutes=30)
}
user_defined_macros = {
    "project_id": "cy-artifacts"
}


def _some_heavy_task():
    print("Do some operation...")
    time.sleep(1)
    print("Done!")


# Simple target DAG
with models.DAG(
    f"target_dag_{VERSION}",
    start_date=datetime(2023, 8, 1),
    # Not scheduled, trigger only
    schedule_interval=None,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30)
) as target_dag:
    
    some_heavy_task = PythonOperator(
        task_id="some_heavy_task", python_callable=_some_heavy_task
    )

    some_heavy_task