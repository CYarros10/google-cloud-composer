"""Example DAG demonstrating the usage of the TaskGroup."""

from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="demo_af_task_group",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",
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
    description="This Airflow DAG demonstrates the usage of the TaskGroup.",
    tags=["demo", "airflow", "taskgroup"],
) as dag:
    start = EmptyOperator(task_id="start")
    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command="echo 1")
        task_3 = EmptyOperator(task_id="task_3")
        task_1 >> [task_2, task_3]
    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_1 = EmptyOperator(task_id="task_1")
        with TaskGroup(
            "inner_section_2", tooltip="Tasks for inner_section2"
        ) as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command="echo 1")
            task_3 = EmptyOperator(task_id="task_3")
            task_4 = EmptyOperator(task_id="task_4")
            [task_2, task_3] >> task_4
    end = EmptyOperator(task_id="end")
    start >> section_1 >> section_2 >> end
