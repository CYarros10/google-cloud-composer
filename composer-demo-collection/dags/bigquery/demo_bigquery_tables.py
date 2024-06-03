"""
Example Airflow DAG for Google BigQuery service testing tables.
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_bigquery_tables"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
SCHEMA_JSON_LOCAL_SRC = str(
    Path(__file__).parent / "resources" / "demo_bq_update_table_schema.json"
)
SCHEMA_JSON_DESTINATION = "update_table_schema.json"
GCS_PATH_TO_SCHEMA_JSON = f"gs://{BUCKET_NAME}/{SCHEMA_JSON_DESTINATION}"
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
    description="This Airflow DAG tests BigQuery operations including table creation, modification, and deletion.",
    tags=["demo", "google_cloud", "bigquery"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_schema_json = LocalFilesystemToGCSOperator(
        task_id="upload_schema_json",
        src=SCHEMA_JSON_LOCAL_SRC,
        dst=SCHEMA_JSON_DESTINATION,
        bucket=BUCKET_NAME,
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )
    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_view",
        dataset_id=DATASET_NAME,
        table_id="test_view",
        view={
            "query": f"SELECT * FROM `{PROJECT_ID}.{DATASET_NAME}.test_table`",
            "useLegacySql": False,
        },
    )
    create_materialized_view = BigQueryCreateEmptyTableOperator(
        task_id="create_materialized_view",
        dataset_id=DATASET_NAME,
        table_id="test_materialized_view",
        materialized_view={
            "query": f"SELECT SUM(salary) AS sum_salary FROM `{PROJECT_ID}.{DATASET_NAME}.test_table`",
            "enableRefresh": True,
            "refreshIntervalMs": 2000000,
        },
    )
    delete_view = BigQueryDeleteTableOperator(
        task_id="delete_view",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_view",
    )
    update_table = BigQueryUpdateTableOperator(
        task_id="update_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        fields=["friendlyName", "description"],
        table_resource={
            "friendlyName": "Updated Table",
            "description": "Updated Table",
        },
    )
    upsert_table = BigQueryUpsertTableOperator(
        task_id="upsert_table",
        dataset_id=DATASET_NAME,
        table_resource={
            "tableReference": {"tableId": "test_table_id"},
            "expirationTime": (int(time.time()) + 300) * 1000,
        },
    )
    update_table_schema = BigQueryUpdateTableSchemaOperator(
        task_id="update_table_schema",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields_updates=[
            {"name": "emp_name", "description": "Name of employee"},
            {"name": "salary", "description": "Monthly salary in USD"},
        ],
    )
    update_table_schema_json = BigQueryCreateEmptyTableOperator(
        task_id="update_table_schema_json",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        gcs_schema_object=GCS_PATH_TO_SCHEMA_JSON,
    )
    delete_materialized_view = BigQueryDeleteTableOperator(
        task_id="delete_materialized_view",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_materialized_view",
    )
    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", dataset_id=DATASET_NAME
    )
    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"description": "Updated dataset"},
    )
    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.test_table",
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    delete_dataset.trigger_rule = TriggerRule.ALL_DONE
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_bucket
        >> create_dataset
        >> upload_schema_json
        >> update_dataset
        >> create_table
        >> create_view
        >> create_materialized_view
        >> [get_dataset_tables, delete_view]
        >> update_table
        >> upsert_table
        >> update_table_schema
        >> update_table_schema_json
        >> delete_materialized_view
        >> delete_table
        >> delete_bucket
        >> [delete_dataset, end]
    )
