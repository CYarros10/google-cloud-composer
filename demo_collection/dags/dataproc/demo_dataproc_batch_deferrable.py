"""
Example Airflow DAG for DataprocSubmitJobOperator with spark job
in deferrable mode.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_dataproc_batch_deferrable"
LOCATION_REGION = "us-central1"
BATCH_ID = f"batch-{ENV_ID}-{DAG_ID}".replace("_", "-")
BATCH_CONFIG = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    }
}
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
    description="This Airflow DAG creates a Spark job using the DataprocCreateBatchOperator in deferrable mode.",
    tags=["demo", "google_cloud", "dataproc", "batch", "spark"],
) as dag:
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch=BATCH_CONFIG,
        batch_id=BATCH_ID,
        deferrable=True,
    )
    get_batch = DataprocGetBatchOperator(
        task_id="get_batch", project_id=PROJECT_ID, region=LOCATION_REGION, batch_id=BATCH_ID
    )
    delete_batch = DataprocDeleteBatchOperator(
        task_id="delete_batch", project_id=PROJECT_ID, region=LOCATION_REGION, batch_id=BATCH_ID
    )
    delete_batch.trigger_rule = TriggerRule.ALL_DONE
    create_batch >> get_batch >> delete_batch
