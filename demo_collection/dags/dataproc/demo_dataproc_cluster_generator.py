"""
Example Airflow DAG testing Dataproc
operators for managing a cluster and submitting jobs.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
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
PROJECT_ID = "your-project"
DAG_ID = "demo_dataproc_cluster_generation"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}".replace("_", "-")
LOCATION_REGION = "us-central1"
ZONE = f"{LOCATION_REGION}-b"
INIT_FILE_SRC = str(Path(__file__).parent / "resources" / "pip-install.sh")
INIT_FILE = "pip-install.sh"
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n1-standard-4",
    master_disk_size=32,
    worker_machine_type="n1-standard-4",
    worker_disk_size=32,
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[f"gs://{BUCKET_NAME}/{INIT_FILE}"],
    metadata={"PIP_PACKAGES": "pyyaml requests pandas openpyxl"},
    num_preemptible_workers=1,
    preemptibility="PREEMPTIBLE",
).make()
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
    description=" Airflow DAG for Dataproc cluster management and job submission.",
    tags=["demo", "google_cloud", "dataproc"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=INIT_FILE_SRC, dst=INIT_FILE, bucket=BUCKET_NAME
    )
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
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
    (
        create_bucket
        >> upload_file
        >> create_dataproc_cluster
        >> [delete_cluster, delete_bucket]
    )
