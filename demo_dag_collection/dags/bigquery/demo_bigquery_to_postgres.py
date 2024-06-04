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
from airflow.providers.google.cloud.transfers.bigquery_to_postgres import (
    BigQueryToPostgresOperator,
)

ENV_ID = "composer"
DAG_ID = "demo_bigquery_to_postgres"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
TABLE = "table_42"
destination_table = "postgres_table_test"
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
    description="This Airflow DAG demonstrates transferring data from BigQuery to PostgreSQL.",
    tags=["demo", "google_cloud", "bigquery", "postgres"],
) as dag:
    bigquery_to_postgres = BigQueryToPostgresOperator(
        task_id="bigquery_to_postgres",
        dataset_table=f"{DATASET_NAME}.{TABLE}",
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
    create_dataset >> create_table >> bigquery_to_postgres >> [delete_dataset, end]
