"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models

# ----- Google Cloud Storage Airflow Imports
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import (
    GCSToGCSOperator
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)

from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator
)

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator
)

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_1"

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
    "sla": timedelta(minutes=25),
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"data_transfers_dag_{VERSION}",
    description="Sample DAG for various Cloud Storage tasks.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:

    src_local_path='/home/airflow/gcs/dags/data'
    gcs_bucket_name="airflow-reporting-cy"

    # Airflow Local File --> Google Cloud Storage location
    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs",
        src=[
            f"{src_local_path}/cities_0.csv",
            f"{src_local_path}/cities_1.csv",
            f"{src_local_path}/cities_2.csv"
        ],
        dst="data_transfers_dag/src/",
        bucket=gcs_bucket_name,
    )

    dst_gcs_path="data_transfers_dag/dst"

    # Google Cloud Storage source --> Google Cloud Storage destination
    gcs_to_gcs = GCSToGCSOperator(
        task_id="gcs_to_gcs",
        source_bucket=gcs_bucket_name,
        source_object="data_transfers_dag/src/",
        destination_bucket=gcs_bucket_name,
        destination_object="data_transfers_dag/dst/",
        match_glob="**/c*.csv",
        move_object=False,
    )

    # Google Cloud Storage location --> BigQuery Table
    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=gcs_bucket_name,
        source_objects=[
            "data_transfers_dag/dst/cities_0.csv",
            "data_transfers_dag/dst/cities_1.csv",
            "data_transfers_dag/dst/cities_2.csv"
        ],
        source_format="CSV",
        destination_project_dataset_table="sandbox.gcs_to_bq_table",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition=f"WRITE_TRUNCATE",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        field_delimiter=",",
        execution_timeout=timedelta(minutes=10),
        schema_fields=[
            {
                "name": "name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "post_abbr",
                "type": "STRING",
            },
            {
                "name": "zip",
                "type": "INT64",
            },
            {
                "name": "phonecode",
                "type": "INT64",
            },
        ]
    )

    # Bigquery Table --> Bigquery Table
    bq_to_bq = BigQueryToBigQueryOperator(
        task_id="bq_to_bq",
        source_project_dataset_tables="sandbox.data_transfer_dag_src_table",
        destination_project_dataset_table="sandbox.data_transfer_dag_dst_table",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition=f"WRITE_TRUNCATE",
    )

    # Bigquery Table -->  Google Cloud Storage location
    bq_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table="sandbox.data_transfer_dag_dst_table",
        destination_cloud_storage_uris=[f"gs://{gcs_bucket_name}/bq/"],
    )

    local_to_gcs >> gcs_to_gcs >> gcs_to_bq >> bq_to_bq >> bq_to_gcs