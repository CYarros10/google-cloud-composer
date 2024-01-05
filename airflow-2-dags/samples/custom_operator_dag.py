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

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

# -------------------------
# Tags, Default Args, and Macros
# -------------------------
tags = ["application:samples"]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2022, 3, 15),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=1),
}
user_defined_macros = {
    "project_id": "cy-artifacts",
    "region": "us-central1",
    "bq_dataset_id": "health_check_dataset",
    "bq_table_id": "health_check_table",
    "bq_partition_id": "",
}


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
    f"custom_operator_dag_{VERSION}",
    description="Sample DAG for custom operator creation.",
    schedule_interval="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
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
