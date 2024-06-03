"""
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
"""

from datetime import datetime, timedelta
from google.cloud.aiplatform import schema
from google.protobuf.struct_pb2 import Value
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLImageTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_vertex_ai_auto_ml_ops"
LOCATION_REGION = "us-central1"
IMAGE_DISPLAY_NAME = f"auto-ml-image-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-image-model-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
IMAGE_GCS_BUCKET_NAME = f"bucket_image_{DAG_ID}_{ENV_ID}".replace("_", "-")
IMAGE_DATASET = {
    "display_name": f"image-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.image,
    "metadata": Value(string_value="image-dataset"),
}
IMAGE_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.image.single_label_classification,
        "gcs_source": {
            "uris": [f"gs://{IMAGE_GCS_BUCKET_NAME}/vertex-ai/image-dataset.csv"]
        },
    }
]
with DAG(
    f"{DAG_ID}_image_training_job",
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
    description="This Airflow DAG creates a bucket, imports image data, creates an AutoML image training job, and then deletes the bucket and dataset.",
    tags=["demo", "google_cloud", "vertex_ai", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=IMAGE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/datasets",
        destination_bucket=IMAGE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    create_image_dataset = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    image_dataset_id = create_image_dataset.output["dataset_id"]
    import_image_dataset = ImportDataOperator(
        task_id="import_image_data",
        dataset_id=image_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        import_configs=IMAGE_DATA_CONFIG,
    )
    create_auto_ml_image_training_job = CreateAutoMLImageTrainingJobOperator(
        task_id="auto_ml_image_task",
        display_name=IMAGE_DISPLAY_NAME,
        dataset_id=image_dataset_id,
        prediction_type="classification",
        multi_label=False,
        model_type="CLOUD",
        training_fraction_split=0.6,
        validation_fraction_split=0.2,
        test_fraction_split=0.2,
        budget_milli_node_hours=8000,
        model_display_name=MODEL_DISPLAY_NAME,
        disable_early_stopping=False,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_auto_ml_image_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_image_task', key='training_id') }}",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_image_dataset = DeleteDatasetOperator(
        task_id="delete_image_dataset",
        dataset_id=image_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=IMAGE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> move_dataset_file, create_image_dataset]
        >> import_image_dataset
        >> create_auto_ml_image_training_job
        >> delete_auto_ml_image_training_job
        >> delete_image_dataset
        >> delete_bucket
    )
