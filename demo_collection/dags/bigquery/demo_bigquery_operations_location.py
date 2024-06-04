"""
Example Airflow DAG for Google BigQuery service testing data structures with location.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
DAG_ID = "demo_bigquery_operations_location"
LOCATION_REGION = "us-central1"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
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
    description="This Airflow DAG tests BigQuery data structures using a location-aware dataset and table.",
    tags=["demo", "google_cloud", "bigquery"],
) as dag:
    create_dataset_with_location = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_with_location",
        dataset_id=DATASET_NAME,
        location=LOCATION_REGION,
    )
    create_table_with_location = BigQueryCreateEmptyTableOperator(
        task_id="create_table_with_location",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    delete_dataset_with_location = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset_with_location",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_dataset_with_location
        >> create_table_with_location
        >> [delete_dataset_with_location, end]
    )
