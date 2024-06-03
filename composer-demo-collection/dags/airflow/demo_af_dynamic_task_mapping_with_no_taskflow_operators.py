"""Example DAG demonstrating the usage of dynamic task mapping with non-TaskFlow operators."""

from datetime import datetime
from datetime import timedelta
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG


class AddOneOperator(BaseOperator):
    """A custom operator that adds one to the input."""

    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        return self.value + 1


class SumItOperator(BaseOperator):
    """A custom operator that sums the input."""

    template_fields = ("values",)

    def __init__(self, values, **kwargs):
        super().__init__(**kwargs)
        self.values = values

    def execute(self, context):
        total = sum(self.values)
        print(f"Total was {total}")
        return total


with DAG(
    dag_id="dmeo_af_dynamic_task_mapping_with_no_taskflow_operators",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="This Airflow DAG demonstrates dynamic task mapping with non-TaskFlow operators.",
    tags=["demo", "airflow"],
):
    add_one_task = AddOneOperator.partial(task_id="add_one").expand(value=[1, 2, 3])
    sum_it_task = SumItOperator(task_id="sum_it", values=add_one_task.output)
