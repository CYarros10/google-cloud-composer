"""
Example DAG using TrinoToGCSOperator.
"""

import re
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.trino_to_gcs import TrinoToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
GCP_PROJECT_ID = "your-project"
DAG_ID = "demo_trino_to_gcs"
GCS_BUCKET = f"bucket_{DAG_ID}_{ENV_ID}"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
SOURCE_SCHEMA_COLUMNS = "memory.information_schema.columns"
SOURCE_CUSTOMER_TABLE = "tpch.sf1.customer"


def safe_name(s: str) -> str:
    """
    Remove invalid characters for filename
    """
    return re.sub("[^0-9a-zA-Z_]+", "_", s)


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
    description="The example DAG utilizes TrinoToGCSOperator to extract data from Trino and exports it to GCS.",
    tags=["demo", "google_cloud", "gcs", "trino", "bigquery"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset", dataset_id=DATASET_NAME
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=GCS_BUCKET
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=GCS_BUCKET
    )
    trino_to_gcs_basic = TrinoToGCSOperator(
        task_id="trino_to_gcs_basic",
        sql=f"select * from {SOURCE_SCHEMA_COLUMNS}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_SCHEMA_COLUMNS)}.{{}}.json",
    )
    trino_to_gcs_multiple_types = TrinoToGCSOperator(
        task_id="trino_to_gcs_multiple_types",
        sql=f"select * from {SOURCE_SCHEMA_COLUMNS}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_SCHEMA_COLUMNS)}.{{}}.json",
        schema_filename=f"{safe_name(SOURCE_SCHEMA_COLUMNS)}-schema.json",
        gzip=False,
    )
    create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_multiple_types",
        bucket=GCS_BUCKET,
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": f"{safe_name(SOURCE_SCHEMA_COLUMNS)}",
            },
            "schema": {
                "fields": [
                    {"name": "table_catalog", "type": "STRING"},
                    {"name": "table_schema", "type": "STRING"},
                    {"name": "table_name", "type": "STRING"},
                    {"name": "column_name", "type": "STRING"},
                    {"name": "ordinal_position", "type": "INT64"},
                    {"name": "column_default", "type": "STRING"},
                    {"name": "is_nullable", "type": "STRING"},
                    {"name": "data_type", "type": "STRING"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "compression": "NONE",
                "sourceUris": [
                    f"gs://{GCS_BUCKET}/{safe_name(SOURCE_SCHEMA_COLUMNS)}.*.json"
                ],
            },
        },
        source_objects=[f"{safe_name(SOURCE_SCHEMA_COLUMNS)}.*.json"],
        schema_object=f"{safe_name(SOURCE_SCHEMA_COLUMNS)}-schema.json",
    )
    read_data_from_gcs_multiple_types = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs_multiple_types",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.{safe_name(SOURCE_SCHEMA_COLUMNS)}`",
                "useLegacySql": False,
            }
        },
    )
    trino_to_gcs_many_chunks = TrinoToGCSOperator(
        task_id="trino_to_gcs_many_chunks",
        sql=f"select * from {SOURCE_CUSTOMER_TABLE}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_CUSTOMER_TABLE)}.{{}}.json",
        schema_filename=f"{safe_name(SOURCE_CUSTOMER_TABLE)}-schema.json",
        approx_max_file_size_bytes=10000000,
        gzip=False,
    )
    create_external_table_many_chunks = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_many_chunks",
        bucket=GCS_BUCKET,
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": f"{safe_name(SOURCE_CUSTOMER_TABLE)}",
            },
            "schema": {
                "fields": [
                    {"name": "custkey", "type": "INT64"},
                    {"name": "name", "type": "STRING"},
                    {"name": "address", "type": "STRING"},
                    {"name": "nationkey", "type": "INT64"},
                    {"name": "phone", "type": "STRING"},
                    {"name": "acctbal", "type": "FLOAT64"},
                    {"name": "mktsegment", "type": "STRING"},
                    {"name": "comment", "type": "STRING"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "compression": "NONE",
                "sourceUris": [
                    f"gs://{GCS_BUCKET}/{safe_name(SOURCE_CUSTOMER_TABLE)}.*.json"
                ],
            },
        },
        source_objects=[f"{safe_name(SOURCE_CUSTOMER_TABLE)}.*.json"],
        schema_object=f"{safe_name(SOURCE_CUSTOMER_TABLE)}-schema.json",
    )
    read_data_from_gcs_many_chunks = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs_many_chunks",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.{safe_name(SOURCE_CUSTOMER_TABLE)}`",
                "useLegacySql": False,
            }
        },
    )
    trino_to_gcs_csv = TrinoToGCSOperator(
        task_id="trino_to_gcs_csv",
        sql=f"select * from {SOURCE_SCHEMA_COLUMNS}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_SCHEMA_COLUMNS)}.{{}}.csv",
        schema_filename=f"{safe_name(SOURCE_SCHEMA_COLUMNS)}-schema.json",
        export_format="csv",
    )
    end = EmptyOperator(task_id="end")
    (
        [create_dataset, create_bucket]
        >> trino_to_gcs_basic
        >> trino_to_gcs_multiple_types
        >> trino_to_gcs_many_chunks
        >> trino_to_gcs_csv
        >> create_external_table_multiple_types
        >> create_external_table_many_chunks
        >> read_data_from_gcs_multiple_types
        >> read_data_from_gcs_many_chunks
        >> [delete_dataset, delete_bucket]
    )
    read_data_from_gcs_many_chunks >> end
