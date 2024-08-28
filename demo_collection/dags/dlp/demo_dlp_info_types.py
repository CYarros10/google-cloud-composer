"""
Example Airflow DAG that creates and manage Data Loss Prevention info types.
"""

from datetime import datetime, timedelta
from pathlib import Path
from google.cloud.dlp_v2 import StoredInfoTypeConfig
from google.cloud.dlp_v2.types import ContentItem, InspectConfig, InspectTemplate
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateStoredInfoTypeOperator,
    CloudDLPDeleteStoredInfoTypeOperator,
    CloudDLPGetStoredInfoTypeOperator,
    CloudDLPListInfoTypesOperator,
    CloudDLPListStoredInfoTypesOperator,
    CloudDLPUpdateStoredInfoTypeOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dlp_info_types"
ENV_ID = "composer"
PROJECT_ID = "your-project"
TEMPLATE_ID = f"dlp-inspect-info-{ENV_ID}"
ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(
    info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}]
)
INSPECT_TEMPLATE = InspectTemplate(inspect_config=INSPECT_CONFIG)
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "dictionary.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
FILE_SET = "tmp/"
DICTIONARY_PATH = FILE_SET + FILE_NAME
OBJECT_GCS_URI = f"gs://{BUCKET_NAME}/{FILE_SET}"
OBJECT_GCS_OUTPUT_URI = OBJECT_GCS_URI + FILE_NAME
CUSTOM_INFO_TYPE_ID = "custom_info_type"
CUSTOM_INFO_TYPES = StoredInfoTypeConfig(
    {
        "large_custom_dictionary": {
            "output_path": {"path": OBJECT_GCS_OUTPUT_URI},
            "cloud_storage_file_set": {"url": f"{OBJECT_GCS_URI}*"},
        }
    }
)
UPDATE_CUSTOM_INFO_TYPE = {
    "large_custom_dictionary": {
        "output_path": {"path": OBJECT_GCS_OUTPUT_URI},
        "cloud_storage_file_set": {"url": f"{OBJECT_GCS_URI}*"},
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
    description="This Airflow DAG manages Data Loss Prevention (DLP) Info Types: creating, listing, retrieving, updating, and deleting.",
    tags=["demo", "google_cloud", "dlp"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=DICTIONARY_PATH,
        bucket=BUCKET_NAME,
    )
    list_possible_info_types = CloudDLPListInfoTypesOperator(task_id="list_info_types")
    create_info_type = CloudDLPCreateStoredInfoTypeOperator(
        project_id=PROJECT_ID,
        config=CUSTOM_INFO_TYPES,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
        task_id="create_info_type",
    )
    list_stored_info_types = CloudDLPListStoredInfoTypesOperator(
        task_id="list_stored_info_types", project_id=PROJECT_ID
    )
    get_stored_info_type = CloudDLPGetStoredInfoTypeOperator(
        task_id="get_stored_info_type",
        project_id=PROJECT_ID,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
    )
    update_info_type = CloudDLPUpdateStoredInfoTypeOperator(
        project_id=PROJECT_ID,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
        config=UPDATE_CUSTOM_INFO_TYPE,
        task_id="update_info_type",
    )
    delete_info_type = CloudDLPDeleteStoredInfoTypeOperator(
        project_id=PROJECT_ID,
        stored_info_type_id=CUSTOM_INFO_TYPE_ID,
        task_id="delete_info_type",
    )
    delete_info_type.trigger_rule = TriggerRule.ALL_DONE
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> upload_file
        >> list_possible_info_types
        >> create_info_type
        >> list_stored_info_types
        >> get_stored_info_type
        >> update_info_type
        >> delete_info_type
        >> delete_bucket
    )
