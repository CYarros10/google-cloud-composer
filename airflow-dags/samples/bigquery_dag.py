"""
"""

from datetime import timedelta, datetime
from airflow import models
from typing import Sequence, Any
from airflow.utils.task_group import TaskGroup

from airflow.contrib.operators.bigquery_operator import (
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateEmptyDatasetOperator,
)

from airflow.contrib.operators.bigquery_check_operator import (
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
)

from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryTableCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteDatasetOperator,
)

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

# -------------------------
# Tags, Default Args, and Macros
# -------------------------
tags = []

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
    f"bigquery_dag_{VERSION}",
    description="",
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
        pre_bq_delete_dataset = BigQueryDeleteDatasetOperator(
            task_id="pre_bq_delete_dataset",
            dataset_id="{{bq_dataset_id}}",
            delete_contents=True,
        )

        bq_create_empty_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="bq_create_empty_dataset",
            dataset_id="{{ bq_dataset_id }}",
            trigger_rule="all_done",
        )

        bq_create_empty_table = BigQueryCreateEmptyTableOperator(
            task_id="bq_create_empty_table",
            dataset_id="{{bq_dataset_id}}",
            table_id="{{bq_table_id}}",
            schema_fields=[
                {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
            ],
        )

        bq_insert_job = BigQueryInsertJobOperator(
            task_id="bq_insert_job",
            configuration={
                "query": {
                    "query": """
                        INSERT INTO {dataset}.{table} (emp_name,salary)
                        VALUES(\'john\',12345)
                    """.format(
                        dataset="{{ bq_dataset_id }}", table="{{ bq_table_id }}"
                    ),
                    "useLegacySql": False,
                }
            },
            location="US",
        )

        bq_get_dataset = BigQueryGetDatasetOperator(
            task_id="bq_get_dataset",
            project_id="{{project_id}}",
            dataset_id="{{bq_dataset_id}}",
        )

        bq_get_dataset_tables = BigQueryGetDatasetTablesOperator(
            task_id="bq_get_dataset_tables",
            dataset_id="{{bq_dataset_id}}",
        )

        bq_check_table_exists = BigQueryTableExistenceSensor(
            task_id="bq_check_table_exists",
            project_id="{{project_id}}",
            dataset_id="{{bq_dataset_id}}",
            table_id="{{bq_table_id}}",
        )

        bq_check_data = BigQueryCheckOperator(
            task_id="bq_check_data",
            sql="SELECT COUNT(*) FROM {{ bq_dataset_id }}.{{ bq_table_id }}",
            location="US",
        )

        bq_check_value = BigQueryValueCheckOperator(
            task_id="bq_check_value",
            sql="SELECT COUNT(*) FROM `{{ bq_dataset_id }}.{{ bq_table_id }}`",
            pass_value=1,
            location="US",
        )

        # Check if a table partition exists
        bq_check_table_partition_exists = BigQueryTablePartitionExistenceSensor(
            task_id="bq_check_table_partition_exists",
            project_id="{{project_id}}",
            dataset_id="{{bq_dataset_id}}",
            table_id="{{bq_table_id}}",
            partition_id="{{bq_partition_id}}",
        )

        # Check Columns
        bq_check_column = CustomOperatorColumnCheck(
            task_id="bq_column_check",
            table="{{bq_dataset_id}}.{{bq_table_id}}",
            column_mapping={"salary": {"min": {"greater_than": 0}}},
        )

        # Table-level Data Quality Check
        bq_check_table_data_quality = CustomOperatorRowCheck(
            task_id="bq_table_check",
            table="{{bq_dataset_id}}.{{bq_table_id}}",
            checks={"row_count_check": {"check_statement": "COUNT(*) = 1"}},
        )

        post_bq_delete_table = BigQueryDeleteTableOperator(
            task_id="post_bq_delete_table",
            deletion_dataset_table="{{bq_dataset_id}}.{{bq_table_id}}",
            trigger_rule="all_done",
        )

        post_bq_delete_dataset = BigQueryDeleteDatasetOperator(
            task_id="post_bq_delete_dataset",
            dataset_id="{{bq_dataset_id}}",
            delete_contents=True,
            trigger_rule="all_done",
        )

        (
            pre_bq_delete_dataset
            >> bq_create_empty_dataset
            >> bq_create_empty_table
            >> [
                bq_get_dataset_tables,
                bq_check_table_exists,
                bq_get_dataset,
            ]
        )
        bq_check_table_exists >> bq_insert_job
        (
            bq_insert_job
            >> [
                bq_check_data,
                bq_check_value,
                bq_check_column,
                bq_check_table_partition_exists,
                bq_check_table_data_quality,
            ]
            >> post_bq_delete_table
            >> post_bq_delete_dataset
        )
