"""
Example DAG demonstrating the usage of BranchDayOfWeekOperator.
"""

import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay

with DAG(
    dag_id="demo_af_weekday_branch_operator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@once",
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
    description="This Airflow DAG demonstrates the usage of BranchDayOfWeekOperator to conditionally execute tasks based on the day of the week.",
    tags=["demo", "airflow", "branching"],
) as dag:
    empty_task_1 = EmptyOperator(task_id="branch_true")
    empty_task_2 = EmptyOperator(task_id="branch_false")
    empty_task_3 = EmptyOperator(task_id="branch_weekend")
    empty_task_4 = EmptyOperator(task_id="branch_mid_week")
    branch = BranchDayOfWeekOperator(
        task_id="make_choice",
        follow_task_ids_if_true="branch_true",
        follow_task_ids_if_false="branch_false",
        week_day="Monday",
    )
    branch_weekend = BranchDayOfWeekOperator(
        task_id="make_weekend_choice",
        follow_task_ids_if_true="branch_weekend",
        follow_task_ids_if_false="branch_mid_week",
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
    )
    branch >> [empty_task_1, empty_task_2]
    empty_task_2 >> branch_weekend >> [empty_task_3, empty_task_4]
