"""
Example Airflow DAG for Google BigQuery service testing dataset operations.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
DAG_ID = "demo_bigquery_dataset"
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
    description="This Airflow DAG creates, updates, retrieves, and deletes a dataset in Google BigQuery.",
    tags=["demo", "google_cloud", "bigquery"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"description": "Updated dataset"},
    )
    get_dataset = BigQueryGetDatasetOperator(
        task_id="get-dataset", dataset_id=DATASET_NAME
    )
    get_dataset_result = BashOperator(
        task_id="get_dataset_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get-dataset')['id'] }}\"",
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    delete_dataset.trigger_rule = TriggerRule.ALL_DONE
    end = EmptyOperator(task_id="end")
    (
        create_dataset
        >> update_dataset
        >> get_dataset
        >> get_dataset_result
        >> [delete_dataset, end]
    )
