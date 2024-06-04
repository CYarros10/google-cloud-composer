"""Example DAG demonstrating the usage of setup and teardown tasks."""

from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="demo_af_setup_teardown",
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
    description="This Airflow DAG demonstrates the usage of setup and teardown tasks.",
    tags=["demo", "airflow", "teardown"],
) as dag:
    root_setup = BashOperator(
        task_id="root_setup", bash_command="echo 'Hello from root_setup'"
    ).as_setup()
    root_normal = BashOperator(
        task_id="normal", bash_command="echo 'I am just a normal task'"
    )
    root_teardown = BashOperator(
        task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'"
    ).as_teardown(setups=root_setup)
    root_setup >> root_normal >> root_teardown
    with TaskGroup("section_1") as section_1:
        inner_setup = BashOperator(
            task_id="taskgroup_setup", bash_command="echo 'Hello from taskgroup_setup'"
        ).as_setup()
        inner_normal = BashOperator(
            task_id="normal", bash_command="echo 'I am just a normal task'"
        )
        inner_teardown = BashOperator(
            task_id="taskgroup_teardown",
            bash_command="echo 'Hello from taskgroup_teardown'",
        ).as_teardown(setups=inner_setup)
        inner_setup >> inner_normal >> inner_teardown
    root_normal >> section_1
