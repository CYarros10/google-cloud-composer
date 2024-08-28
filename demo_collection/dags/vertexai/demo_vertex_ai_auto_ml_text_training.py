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
    CreateAutoMLTextTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_vertex_ai_auto_ml_ops"
LOCATION_REGION = "us-central1"
TEXT_DISPLAY_NAME = f"auto-ml-text-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-text-model-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
TEXT_GCS_BUCKET_NAME = f"bucket_text_{DAG_ID}_{ENV_ID}".replace("_", "-")
TEXT_DATASET = {
    "display_name": f"text-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="text-dataset"),
}
TEXT_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.text.single_label_classification,
        "gcs_source": {
            "uris": [f"gs://{TEXT_GCS_BUCKET_NAME}/vertex-ai/text-dataset.csv"]
        },
    }
]
with DAG(
    f"{DAG_ID}_text_training_job",
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
    description="This Airflow DAG creates a Vertex AI AutoML Text training job, imports data, and cleans up resources.",
    tags=["demo", "google_cloud", "vertex_ai", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=TEXT_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/datasets",
        destination_bucket=TEXT_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    create_text_dataset = CreateDatasetOperator(
        task_id="text_dataset",
        dataset=TEXT_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    text_dataset_id = create_text_dataset.output["dataset_id"]
    import_text_dataset = ImportDataOperator(
        task_id="import_text_data",
        dataset_id=text_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        import_configs=TEXT_DATA_CONFIG,
    )
    create_auto_ml_text_training_job = CreateAutoMLTextTrainingJobOperator(
        task_id="auto_ml_text_task",
        display_name=TEXT_DISPLAY_NAME,
        prediction_type="classification",
        multi_label=False,
        dataset_id=text_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        training_fraction_split=0.7,
        validation_fraction_split=0.2,
        test_fraction_split=0.1,
        sync=True,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_auto_ml_text_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_text_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_text_task', key='training_id') }}",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_text_dataset = DeleteDatasetOperator(
        task_id="delete_text_dataset",
        dataset_id=text_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=TEXT_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> move_dataset_file, create_text_dataset]
        >> import_text_dataset
        >> create_auto_ml_text_training_job
        >> delete_auto_ml_text_training_job
        >> delete_text_dataset
        >> delete_bucket
    )
