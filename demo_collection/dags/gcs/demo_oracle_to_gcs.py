from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_oracle_to_gcs"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILENAME = "test_file"
SQL_QUERY = "SELECT * from test_table"
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
    description="The DAG exports data from an Oracle database to a GCS bucket and then deletes the bucket.",
    tags=["demo", "google_cloud", "gcs", "oracle"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_oracle_to_gcs = OracleToGCSOperator(
        task_id="oracle_to_gcs",
        sql=SQL_QUERY,
        bucket=BUCKET_NAME,
        filename=FILENAME,
        export_format="csv",
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    create_bucket >> upload_oracle_to_gcs >> delete_bucket
    upload_oracle_to_gcs >> end
