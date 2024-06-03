"""
Example DAG demonstrating setting up inter-DAG dependencies using ExternalTaskSensor and
ExternalTaskMarker.

In this example, child_task1 in example_external_task_marker_child depends on parent_task in
example_external_task_marker_parent. When parent_task is cleared with 'Recursive' selected,
the presence of ExternalTaskMarker tells Airflow to clear child_task1 and its downstream tasks.

ExternalTaskSensor will keep poking for the status of remote ExternalTaskMarker task at a regular
interval till one of the following will happen:

ExternalTaskMarker reaches the states mentioned in the allowed_states list.
In this case, ExternalTaskSensor will exit with a success status code

ExternalTaskMarker reaches the states mentioned in the failed_states list
In this case, ExternalTaskSensor will raise an AirflowException and user need to handle this
with multiple downstream tasks

ExternalTaskSensor times out. In this case, ExternalTaskSensor will raise AirflowSkipException
or AirflowSensorTimeout exception

"""

import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

with DAG(
    dag_id="demo_af_external_task_marker_parent",
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
    description="This Airflow DAG demonstrates setting up inter-DAG dependencies using ExternalTaskSensor and ExternalTaskMarker.",
    tags=["demo", "airflow"],
) as parent_dag:
    parent_task = ExternalTaskMarker(
        task_id="parent_task",
        external_dag_id="demo_af_external_task_marker_child",
        external_task_id="child_task1",
    )
with DAG(
    dag_id="demo_af_external_task_marker_child",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=120),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="This Airflow DAG demonstrates setting up inter-DAG dependencies using ExternalTaskSensor and ExternalTaskMarker.",
    tags=["demo", "airflow", "sensors"],
) as child_dag:
    child_task1 = ExternalTaskSensor(
        task_id="child_task1",
        external_dag_id=parent_dag.dag_id,
        external_task_id=parent_task.task_id,
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
    )
    child_task2 = ExternalTaskSensor(
        task_id="child_task2",
        external_dag_id=parent_dag.dag_id,
        external_task_group_id="parent_dag_task_group_id",
        timeout=600,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
    )
    child_task3 = EmptyOperator(task_id="child_task3")
    child_task1 >> child_task2 >> child_task3
