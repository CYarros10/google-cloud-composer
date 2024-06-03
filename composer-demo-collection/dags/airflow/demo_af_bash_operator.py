"""Example DAG demonstrating the usage of the BashOperator."""

from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="demo_af_bash_operator",
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
    params={"example_key": "example_value"},
    description="This Airflow DAG demonstrates the BashOperator with loops and templating.",
    tags=["demo", "airflow", "bash"],
) as dag:
    run_this_last = EmptyOperator(task_id="run_this_last")
    run_this = BashOperator(task_id="run_after_loop", bash_command="echo 1")
    run_this >> run_this_last
    for i in range(3):
        task = BashOperator(
            task_id=f"runme_{i}",
            bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        )
        task >> run_this
    also_run_this = BashOperator(
        task_id="also_run_this",
        bash_command='echo "ti_key={{ task_instance_key_str }}"',
    )
    also_run_this >> run_this_last
this_will_skip = BashOperator(
    task_id="this_will_skip", bash_command='echo "hello world"; exit 99;', dag=dag
)
this_will_skip >> run_this_last
