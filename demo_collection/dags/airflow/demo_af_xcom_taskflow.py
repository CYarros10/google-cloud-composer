"""Example DAG demonstrating the usage of XComs."""

from datetime import timedelta
import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator

value_1 = [1, 2, 3]
value_2 = {"a": "b"}


@task
def push(ti=None):
    """Pushes an XCom without a specific target"""
    ti.xcom_push(key="value from pusher 1", value=value_1)


@task
def push_by_returning():
    """Pushes an XCom without a specific target, just by returning it"""
    return value_2


def _compare_values(pulled_value, check_value):
    if pulled_value != check_value:
        raise ValueError(f"The two values differ {pulled_value} and {check_value}")


@task
def puller(pulled_value_2, ti=None):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""
    pulled_value_1 = ti.xcom_pull(task_ids="push", key="value from pusher 1")
    _compare_values(pulled_value_1, value_1)
    _compare_values(pulled_value_2, value_2)


@task
def pull_value_from_bash_push(ti=None):
    bash_pushed_via_return_value = ti.xcom_pull(
        key="return_value", task_ids="bash_push"
    )
    bash_manually_pushed_value = ti.xcom_pull(
        key="manually_pushed_value", task_ids="bash_push"
    )
    print(
        f"The xcom value pushed by task push via return value is {bash_pushed_via_return_value}"
    )
    print(
        f"The xcom value pushed by task push manually is {bash_manually_pushed_value}"
    )


with DAG(
    dag_id="demo_af_xcom_taskflow",
    schedule="@once",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
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
    description="This Airflow DAG demonstrates the usage of XComs.",
    tags=["demo", "airflow", "xcom", "bash"],
) as dag:
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command='echo "bash_push demo"  && echo "Manually set xcom value {{ ti.xcom_push(key="manually_pushed_value", value="manually_pushed_value") }}" && echo "value_by_return"',
    )
    bash_pull = BashOperator(
        task_id="bash_pull",
        bash_command=f'''echo "bash pull demo" && echo "The xcom pushed manually is {XComArg(bash_push, key='manually_pushed_value')}" && echo "The returned_value xcom is {XComArg(bash_push)}" && echo "finished"''',
        do_xcom_push=False,
    )
    python_pull_from_bash = pull_value_from_bash_push()
    [bash_pull, python_pull_from_bash] << bash_push
    puller(push_by_returning()) << push()
