"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
from typing import Sequence, Any
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryTableCheckOperator,
    BigQueryColumnCheckOperator
)

# -------------------------
# Callback Functions / Custom Operators
# -------------------------
class CustomOperatorColumnCheck(BigQueryColumnCheckOperator):
    """
    adds table as a templated field to BigQueryColumnCheckOperator
    """

    template_fields: Sequence[str] = (
        *BigQueryColumnCheckOperator.template_fields,
        "table",
        "sql",
    )
    template_fields_renderers = {"sql": "sql"}

    def __init__(self, *, table: str, **kwargs: Any) -> None:
        super().__init__(table=table, **kwargs)
        self.table = table


class CustomOperatorRowCheck(BigQueryTableCheckOperator):
    """
    adds table as a templated field to BigQueryTableCheckOperator
    """

    template_fields: Sequence[str] = (
        *BigQueryTableCheckOperator.template_fields,
        "table",
        "sql",
    )
    template_fields_renderers = {"sql": "sql"}

    def __init__(self, *, table: str, **kwargs: Any) -> None:
        super().__init__(table=table, **kwargs)
        self.table = table


# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"demo_custom_operator",
    schedule_interval="@once",
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
        "sla": timedelta(minutes=55),
    },
    description="This Airflow DAG demonstrates the customizing existing operators.",
    tags=["demo", "airflow", "bigquery", "custom"],
) as dag:
    
    # -------------------------------------------------------------------------
    # BigQuery
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="bigquery_tg1") as bigquery_tg1:

        # Check Columns extends existing operator with more templated field options
        bq_check_column = CustomOperatorColumnCheck(
            task_id="bq_column_check",
            table="{{bq_dataset_id}}.{{bq_table_id}}",
            column_mapping={"salary": {"min": {"greater_than": 0}}},
        )

        # Check Rows extends existing operator with more templated field options
        bq_check_table_data_quality = CustomOperatorRowCheck(
            task_id="bq_table_check",
            table="{{bq_dataset_id}}.{{bq_table_id}}",
            checks={"row_count_check": {"check_statement": "COUNT(*) = 1"}},
        )
