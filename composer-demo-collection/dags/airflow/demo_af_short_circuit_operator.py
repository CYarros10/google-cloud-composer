"""Example DAG demonstrating the usage of the ShortCircuitOperator."""

from datetime import timedelta
import pendulum
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="demo_af_short_circuit_operator",
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
    description="This Airflow DAG demonstrates the ShortCircuitOperator, which allows for conditional execution of downstream tasks based on a Python callable.",
    tags=["demo", "airflow"],
):
    cond_true = ShortCircuitOperator(
        task_id="condition_is_True", python_callable=lambda: True
    )
    cond_false = ShortCircuitOperator(
        task_id="condition_is_False", python_callable=lambda: False
    )
    ds_true = [EmptyOperator(task_id=f"true_{i}") for i in [1, 2]]
    ds_false = [EmptyOperator(task_id=f"false_{i}") for i in [1, 2]]
    chain(cond_true, *ds_true)
    chain(cond_false, *ds_false)
    [task_1, task_2, task_3, task_4, task_5, task_6] = [
        EmptyOperator(task_id=f"task_{i}") for i in range(1, 7)
    ]
    task_7 = EmptyOperator(task_id="task_7", trigger_rule=TriggerRule.ALL_DONE)
    short_circuit = ShortCircuitOperator(
        task_id="short_circuit",
        ignore_downstream_trigger_rules=False,
        python_callable=lambda: False,
    )
    chain(task_1, [task_2, short_circuit], [task_3, task_4], [task_5, task_6], task_7)
