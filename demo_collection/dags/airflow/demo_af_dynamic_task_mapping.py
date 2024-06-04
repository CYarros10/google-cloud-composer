"""Example DAG demonstrating the usage of dynamic task mapping."""

from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    dag_id="demo_af_dynamic_task_mapping",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="This Airflow DAG demonstrates dynamic task mapping using the `expand` method.",
    tags=["demo", "airflow"],
) as dag:

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)
