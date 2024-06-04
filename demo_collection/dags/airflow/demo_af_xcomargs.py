"""Example DAG demonstrating the usage of the XComArgs."""

import logging
import pendulum
from datetime import timedelta
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)


@task
def generate_value():
    """Empty function"""
    return "Bring me a shrubbery!"


@task
def print_value(value, ts=None):
    """Empty function"""
    log.info("The knights of Ni say: %s (at %s)", value, ts)


with DAG(
    dag_id="demo_af_xcom_args",
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
    description="This Airflow DAG demonstrates the usage of the XComArgs.",
    tags=["demo", "airflow", "xcom", "bash"],
) as dag:
    print_value(generate_value())

with DAG(
    "demo_af_xcom_args_with_operators",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@once",
    description="This Airflow DAG demonstrates the usage of the XComArgs.",
    tags=["demo", "airflow", "xcom", "bash"],
) as dag2:
    bash_op1 = BashOperator(task_id="c", bash_command="echo c")
    bash_op2 = BashOperator(task_id="d", bash_command="echo c")
    xcom_args_a = print_value("first!")
    xcom_args_b = print_value("second!")
    bash_op1 >> xcom_args_a >> xcom_args_b >> bash_op2
