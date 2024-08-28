"""
Example Airflow DAG that show how to use various Dataproc Metastore
operators to manage a service.
"""

from datetime import datetime, timedelta
from pathlib import Path
from google.protobuf.field_mask_pb2 import FieldMask
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreUpdateServiceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dpms"
ENV_ID = "composer"
PROJECT_ID = "your-project"
SERVICE_ID = f"{DAG_ID}-service-{ENV_ID}".replace("_", "-")
METADATA_IMPORT_ID = f"{DAG_ID}-metadata-{ENV_ID}".replace("_", "-")
LOCATION_REGION = "us-central1"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
TIMEOUT = 2400
DB_TYPE = "MYSQL"
DESTINATION_GCS_FOLDER = f"gs://{BUCKET_NAME}/>"
HIVE_FILE_SRC = str(
    Path(__file__).parent.parent / "dataproc" / "resources" / "hive.sql"
)
HIVE_FILE = "data/hive.sql"
GCS_URI = f"gs://{BUCKET_NAME}/data/hive.sql"
SERVICE = {"name": "test-service"}
METADATA_IMPORT = {
    "name": "test-metadata-import",
    "database_dump": {"gcs_uri": GCS_URI, "database_type": DB_TYPE},
}
SERVICE_TO_UPDATE = {
    "labels": {"mylocalmachine": "mylocalmachine", "systemtest": "systemtest"}
}
UPDATE_MASK = FieldMask(paths=["labels"])
with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=120),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=110),
    },
    description="This Airflow DAG manages a Dataproc Metastore service and related resources.",
    tags=["demo", "google_cloud", "dataproc_metastore"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=HIVE_FILE_SRC, dst=HIVE_FILE, bucket=BUCKET_NAME
    )
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    get_service = DataprocMetastoreGetServiceOperator(
        task_id="get_service",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    update_service = DataprocMetastoreUpdateServiceOperator(
        task_id="update_service",
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        region=LOCATION_REGION,
        service=SERVICE_TO_UPDATE,
        update_mask=UPDATE_MASK,
        timeout=TIMEOUT,
    )
    import_metadata = DataprocMetastoreCreateMetadataImportOperator(
        task_id="import_metadata",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        service_id=SERVICE_ID,
        metadata_import=METADATA_IMPORT,
        metadata_import_id=METADATA_IMPORT_ID,
        timeout=TIMEOUT,
    )
    export_metadata = DataprocMetastoreExportMetadataOperator(
        task_id="export_metadata",
        destination_gcs_folder=DESTINATION_GCS_FOLDER,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    delete_service.trigger_rule = TriggerRule.ALL_DONE
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> upload_file
        >> create_service
        >> get_service
        >> update_service
        >> import_metadata
        >> export_metadata
        >> delete_service
        >> delete_bucket
    )
