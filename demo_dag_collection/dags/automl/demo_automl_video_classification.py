"""
Example Airflow DAG that uses Google AutoML services.
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
    CreateAutoMLVideoTrainingJobOperator,
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
DAG_ID = "demo_automl_video_clss"
LOCATION_REGION = "us-central1"
VIDEO_DISPLAY_NAME = f"auto-ml-video-clss-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-video-clss-model-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
VIDEO_GCS_BUCKET_NAME = f"bucket_video_clss_{ENV_ID}".replace("_", "-")
VIDEO_DATASET = {
    "display_name": f"dataset-{DAG_ID}".replace("_", "-"),
    "metadata_schema_uri": schema.dataset.metadata.video,
    "metadata": Value(string_value="video-dataset"),
}
VIDEO_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.video.classification,
        "gcs_source": {
            "uris": [f"gs://{VIDEO_GCS_BUCKET_NAME}/automl/classification.csv"]
        },
    }
]
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
    description="This Airflow DAG creates a bucket and moves a dataset to it. Then it creates a dataset, imports video data and performs a video classification training job. It finally deletes the training job, dataset and the bucket.",
    tags=["demo", "google_cloud", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=VIDEO_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="automl/datasets/video",
        destination_bucket=VIDEO_GCS_BUCKET_NAME,
        destination_object="automl",
        recursive=True,
    )
    create_video_dataset = CreateDatasetOperator(
        task_id="video_dataset",
        dataset=VIDEO_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    video_dataset_id = create_video_dataset.output["dataset_id"]
    import_video_dataset = ImportDataOperator(
        task_id="import_video_data",
        dataset_id=video_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        import_configs=VIDEO_DATA_CONFIG,
    )
    create_auto_ml_video_training_job = CreateAutoMLVideoTrainingJobOperator(
        task_id="auto_ml_video_task",
        display_name=VIDEO_DISPLAY_NAME,
        prediction_type="classification",
        model_type="CLOUD",
        dataset_id=video_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_auto_ml_video_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_video_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_video_task', key='training_id') }}",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_video_dataset = DeleteDatasetOperator(
        task_id="delete_video_dataset",
        dataset_id=video_dataset_id,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=VIDEO_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> move_dataset_file, create_video_dataset]
        >> import_video_dataset
        >> create_auto_ml_video_training_job
        >> delete_auto_ml_video_training_job
        >> delete_video_dataset
        >> delete_bucket
    )
