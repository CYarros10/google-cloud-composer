"""
Example DAG using GCSToBigQueryOperator.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_gcs_to_bigquery_operator"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
TABLE_NAME = "test"
with DAG(
    dag_id=DAG_ID,
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
    description="This DAG uses `GCSToBigQueryOperator` to load the `us-states.csv` from `cloud-samples-data` into `dataset_demo_gcs_to_bigquery_operator_ENV.test`.",
    tags=["demo", "google_cloud", "gcs", "bigquery"],
) as dag:
    create_test_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset",
        dataset_id=DATASET_NAME,
        project_id=PROJECT_ID,
    )
    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example",
        bucket="cloud-samples-data",
        source_objects=["bigquery/us-states/us-states.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    delete_test_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_airflow_test_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    create_test_dataset >> load_csv >> delete_test_dataset
    load_csv >> end
