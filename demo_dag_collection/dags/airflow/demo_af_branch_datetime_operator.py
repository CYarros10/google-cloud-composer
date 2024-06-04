"""
Example DAG demonstrating the usage of DateTimeBranchOperator with datetime as well as time objects as
targets.
"""

import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="demo_af_branch_datetime_operator",
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
    description="This Airflow DAG demonstrates the usage of DateTimeBranchOperator with datetime as well as time objects as targets.",
    tags=["demo", "airflow", "branching"],
) as dag:
    empty_task_11 = EmptyOperator(task_id="date_in_range")
    empty_task_21 = EmptyOperator(task_id="date_outside_range")
    cond1 = BranchDateTimeOperator(
        task_id="datetime_branch",
        follow_task_ids_if_true=["date_in_range"],
        follow_task_ids_if_false=["date_outside_range"],
        target_upper=pendulum.datetime(2020, 10, 10, 15, 0, 0),
        target_lower=pendulum.datetime(2020, 10, 10, 14, 0, 0),
    )
    cond1 >> [empty_task_11, empty_task_21]
with DAG(
    dag_id="demo_branch_datetime_operator_2",
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=120),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=110),
    },
    description="This Airflow DAG demonstrates the usage of DateTimeBranchOperator with datetime as well as time objects as targets.",
    tags=["demo", "airflow", "branching"],
) as dag2:
    empty_task_12 = EmptyOperator(task_id="date_in_range")
    empty_task_22 = EmptyOperator(task_id="date_outside_range")
    cond2 = BranchDateTimeOperator(
        task_id="datetime_branch",
        follow_task_ids_if_true=["date_in_range"],
        follow_task_ids_if_false=["date_outside_range"],
        target_upper=pendulum.time(0, 0, 0),
        target_lower=pendulum.time(15, 0, 0),
    )
    cond2 >> [empty_task_12, empty_task_22]
with DAG(
    dag_id="demo_branch_datetime_operator_3",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    description="This Airflow DAG demonstrates the usage of DateTimeBranchOperator with datetime as well as time objects as targets.",
    tags=["demo", "airflow", "branching"],
) as dag3:
    empty_task_13 = EmptyOperator(task_id="date_in_range")
    empty_task_23 = EmptyOperator(task_id="date_outside_range")
    cond3 = BranchDateTimeOperator(
        task_id="datetime_branch",
        use_task_logical_date=True,
        follow_task_ids_if_true=["date_in_range"],
        follow_task_ids_if_false=["date_outside_range"],
        target_upper=pendulum.datetime(2020, 10, 10, 15, 0, 0),
        target_lower=pendulum.datetime(2020, 10, 10, 14, 0, 0),
    )
    cond3 >> [empty_task_13, empty_task_23]
