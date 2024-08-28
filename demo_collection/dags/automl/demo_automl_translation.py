"""
Example Airflow DAG that uses Google AutoML services.
"""

from datetime import datetime, timedelta
from typing import cast
from google.cloud import storage
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLDeleteModelOperator,
    AutoMLImportDataOperator,
    AutoMLTrainModelOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_automl_translate"
ENV_ID = "composer"
PROJECT_ID = "your-project"
LOCATION_REGION = "us-central1"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
MODEL_NAME = "translate_test_model"
MODEL = {"display_name": MODEL_NAME, "translation_model_metadata": {}}
DATASET_NAME = f"dataset_{DAG_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "translation_dataset_metadata": {
        "source_language_code": "en",
        "target_language_code": "es",
    },
}
CSV_FILE_NAME = "en-es.csv"
TSV_FILE_NAME = "en-es.tsv"
GCS_FILE_PATH = f"automl/datasets/translate/{CSV_FILE_NAME}"
AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/{CSV_FILE_NAME}"
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}
extract_object_id = CloudAutoMLHook.extract_object_id
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=360),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=330),
    },
    user_defined_macros={"extract_object_id": extract_object_id},
    tags=["demo", "google_cloud", "automl"],
    description="This DAG creates a Cloud AutoML Translation dataset and model, using a CSV file as input and then cleans up by deleting the model, dataset, and bucket. ",
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )

    @task
    def upload_csv_file_to_gcs():
        storage_client = storage.Client()
        bucket = storage_client.bucket(RESOURCE_DATA_BUCKET)
        blob = bucket.blob(GCS_FILE_PATH)
        contents = blob.download_as_string().decode()
        updated_contents = contents.replace(
            "template-bucket", DATA_SAMPLE_GCS_BUCKET_NAME
        )
        destination_bucket = storage_client.bucket(DATA_SAMPLE_GCS_BUCKET_NAME)
        destination_blob = destination_bucket.blob(f"automl/{CSV_FILE_NAME}")
        destination_blob.upload_from_string(updated_contents)

    upload_csv_file_to_gcs_task = upload_csv_file_to_gcs()
    copy_dataset_file = GCSToGCSOperator(
        task_id="copy_dataset_file",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object=f"automl/datasets/translate/{TSV_FILE_NAME}",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object=f"automl/{TSV_FILE_NAME}",
    )
    create_dataset = AutoMLCreateDatasetOperator(
        task_id="create_dataset", dataset=DATASET, location=LOCATION_REGION
    )
    dataset_id = cast(str, XComArg(create_dataset, key="dataset_id"))
    import_dataset = AutoMLImportDataOperator(
        task_id="import_dataset",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        input_config=IMPORT_INPUT_CONFIG,
    )
    MODEL["dataset_id"] = dataset_id
    create_model = AutoMLTrainModelOperator(
        task_id="create_model", model=MODEL, location=LOCATION_REGION
    )
    model_id = cast(str, XComArg(create_model, key="model_id"))
    delete_model = AutoMLDeleteModelOperator(
        task_id="delete_model",
        model_id=model_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> upload_csv_file_to_gcs_task >> copy_dataset_file]
        >> create_dataset
        >> import_dataset
        >> create_model
        >> delete_dataset
        >> delete_model
        >> delete_bucket
    )
