"""
Airflow System Test DAG that verifies BigQueryToBigQueryOperator.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
DAG_ID = "demo_bigquery_to_bigquery"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
ORIGIN = "origin"
TARGET = "target"
LOCATION_REGION = "US"
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
    description="This DAG showcases BigQueryToBigQueryOperator by creating a dataset, origin and target tables, copying data from the origin to the target table, then deleting the dataset.",
    tags=["demo", "google_cloud", "bigquery"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, location=LOCATION_REGION
    )
    create_origin_table = BigQueryCreateEmptyTableOperator(
        task_id="create_origin_table",
        dataset_id=DATASET_NAME,
        table_id="origin",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    create_target_table = BigQueryCreateEmptyTableOperator(
        task_id="create_target_table",
        dataset_id=DATASET_NAME,
        table_id="target",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    copy_selected_data = BigQueryToBigQueryOperator(
        task_id="copy_selected_data",
        source_project_dataset_tables=f"{DATASET_NAME}.{ORIGIN}",
        destination_project_dataset_table=f"{DATASET_NAME}.{TARGET}",
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_dataset
        >> [create_origin_table, create_target_table]
        >> copy_selected_data
        >> [delete_dataset, end]
    )
