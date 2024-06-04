"""
Example DAG demonstrating the usage of ``@task.branch`` TaskFlow API decorator with depends_on_past=True,
where tasks may be run or skipped on alternating runs.
"""

import pendulum
from datetime import timedelta
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator


@task.branch()
def should_run(**kwargs) -> str:
    """
    Determine which empty_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    """
    print(
        f"------------- exec dttm = {kwargs['execution_date']} and minute = {kwargs['execution_date'].minute}"
    )
    if kwargs["execution_date"].minute % 2 == 0:
        return "empty_task_1"
    else:
        return "empty_task_2"


with DAG(
    dag_id="demo_af_branch_dop_operator_v3",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=55),
    },
    description="This Airflow DAG demonstrates the usage of the `@task.branch` TaskFlow API decorator with `depends_on_past=True`, where tasks may be run or skipped on alternating runs based",
    tags=["demo", "airflow", "branching"],
) as dag:
    cond = should_run()
    empty_task_1 = EmptyOperator(task_id="empty_task_1")
    empty_task_2 = EmptyOperator(task_id="empty_task_2")
    cond >> [empty_task_1, empty_task_2]
