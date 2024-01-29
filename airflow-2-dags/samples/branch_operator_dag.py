"""
Airflow DAG with a branching task structure.
"""

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

VERSION="0_0_1"

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
tags = ["application:samples"]

with DAG(
    f"branchoperator_dag_{VERSION}",
    description="A DAG that branches",
    default_view="graph",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    

    def branch():
        if datetime.now().weekday() >= 5:
            return 'weekend_task'
        else:
            return 'weekday_task'

    def weekend():
        print("It's the weekend.")


    def weekday():
        print("It's a weekday")

    branch_task = BranchPythonOperator(task_id='branch', python_callable=branch)
    weekend_task = PythonOperator(task_id='weekend_task', python_callable=weekend)
    weekday_task = PythonOperator(task_id='weekday_task', python_callable=weekday)

    branch_task >> [weekend_task, weekday_task]