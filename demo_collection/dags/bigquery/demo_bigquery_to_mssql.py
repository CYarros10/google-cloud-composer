"""
Example Airflow DAG for Google BigQuery service.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_mssql import (
    BigQueryToMsSqlOperator,
)

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_bigquery_to_mssql"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
TABLE = "table_42"
destination_table = "mssql_table_test"
with DAG(
    DAG_ID,
    schedule="@once",
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
    description="The DAG transfers data from a BigQuery table to a target MSSQL table at once.",
    tags=["demo", "google_cloud", "bigquery", "mssql"],
) as dag:
    bigquery_to_mssql = BigQueryToMsSqlOperator(
        task_id="bigquery_to_mssql",
        source_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE}",
        target_table_name=destination_table,
        replace=False,
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    end = EmptyOperator(task_id="end")
    create_dataset >> create_table >> bigquery_to_mssql >> [delete_dataset, end]
