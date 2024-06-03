"""
Example Airflow DAG that uses Google AutoML services.
"""

from datetime import datetime, timedelta
from google.cloud.aiplatform import schema
from google.protobuf.struct_pb2 import Value
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
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
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_automl_text_cls"
LOCATION_REGION = "us-central1"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
TEXT_CLSS_DISPLAY_NAME = f"{DAG_ID}-{ENV_ID}".replace("_", "-")
AUTOML_DATASET_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/classification.csv"
MODEL_NAME = f"{DAG_ID}-{ENV_ID}".replace("_", "-")
DATASET_NAME = f"dataset_{DAG_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="clss-dataset"),
}
DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.text.single_label_classification,
        "gcs_source": {"uris": [AUTOML_DATASET_BUCKET]},
    }
]
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
    description="DAG automates AutoML Text Classification workflow including data prep, training, and cleanup.",
    tags=["demo", "google_cloud", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/automl/datasets/text",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="automl",
        recursive=True,
    )
    create_clss_dataset = CreateDatasetOperator(
        task_id="create_clss_dataset",
        dataset=DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    clss_dataset_id = create_clss_dataset.output["dataset_id"]
    import_clss_dataset = ImportDataOperator(
        task_id="import_clss_data",
        dataset_id=clss_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        import_configs=DATA_CONFIG,
    )
    create_clss_training_job = CreateAutoMLTextTrainingJobOperator(
        task_id="create_clss_training_job",
        display_name=TEXT_CLSS_DISPLAY_NAME,
        prediction_type="classification",
        multi_label=False,
        dataset_id=clss_dataset_id,
        model_display_name=MODEL_NAME,
        training_fraction_split=0.7,
        validation_fraction_split=0.2,
        test_fraction_split=0.1,
        sync=True,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_clss_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_clss_training_job",
        training_pipeline_id=create_clss_training_job.output["training_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_clss_dataset = DeleteDatasetOperator(
        task_id="delete_clss_dataset",
        dataset_id=clss_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> move_dataset_file, create_clss_dataset]
        >> import_clss_dataset
        >> create_clss_training_job
        >> delete_clss_training_job
        >> delete_clss_dataset
        >> delete_bucket
    )
