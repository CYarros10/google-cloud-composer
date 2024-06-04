"""Example DAG demonstrating the EmptyOperator and a custom EmptySkipOperator which skips by default."""

from datetime import timedelta
import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.utils.context import Context


class EmptySkipOperator(BaseOperator):
    """Empty operator which always skips the task."""

    ui_color = "#e8b7e4"

    def execute(self, context: Context):
        raise AirflowSkipException


def create_test_pipeline(suffix, trigger_rule):
    """
    Instantiate a number of operators for the given DAG.

    :param str suffix: Suffix to append to the operator task_ids
    :param str trigger_rule: TriggerRule for the join task
    :param DAG dag_: The DAG to run the operators on
    """
    skip_operator = EmptySkipOperator(task_id=f"skip_operator_{suffix}")
    always_true = EmptyOperator(task_id=f"always_true_{suffix}")
    join = EmptyOperator(task_id=trigger_rule, trigger_rule=trigger_rule)
    final = EmptyOperator(task_id=f"final_{suffix}")
    skip_operator >> join
    always_true >> join
    join >> final


with DAG(
    dag_id="demo_af_skip_dag",
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
    description="This Airflow DAG demonstrates the EmptyOperator and a custom EmptySkipOperator which skips by default.",
    tags=["demo", "airflow"],
) as dag:
    create_test_pipeline("1", TriggerRule.ALL_SUCCESS)
    create_test_pipeline("2", TriggerRule.ONE_SUCCESS)
