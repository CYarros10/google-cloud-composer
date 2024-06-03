"""
Example Airflow DAG for Dataproc batch operators.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateBatchOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataproc_batch_ps"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
LOCATION_REGION = "us-central1"
CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}".replace("_", "-")
BATCH_ID = f"batch-{ENV_ID}-{DAG_ID}".replace("_", "-")
CLUSTER_GENERATOR_CONFIG_FOR_PHS = ClusterGenerator(
    project_id=PROJECT_ID,
    region=LOCATION_REGION,
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    num_workers=0,
    properties={"spark:spark.history.fs.logDirectory": f"gs://{BUCKET_NAME}/logging"},
    enable_component_gateway=True,
).make()
BATCH_CONFIG_WITH_PHS = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
    "environment_config": {
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{LOCATION_REGION}/clusters/{CLUSTER_NAME}"
            }
        }
    },
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
    description=" This Airflow DAG utilizes Dataflow for batch operations on a Dataproc cluster with history server configured in a Cloud Storage bucket. \n The DAG encompasses bucket creation, cluster provisioning, batch execution, cluster deletion, and bucket removal.",
    tags=["demo", "google_cloud", "dataproc", "batch", "spark"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster_for_phs",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG_FOR_PHS,
        region=LOCATION_REGION,
        cluster_name=CLUSTER_NAME,
    )
    create_batch = DataprocCreateBatchOperator(
        task_id="create_batch_with_phs",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        batch=BATCH_CONFIG_WITH_PHS,
        batch_id=BATCH_ID,
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_bucket >> create_cluster >> create_batch >> delete_cluster >> delete_bucket
