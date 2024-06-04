"""
Example Airflow DAG for Google Vertex AI service testing Dataset operations.
"""

from datetime import datetime, timedelta
from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ExportDataOperator,
    GetDatasetOperator,
    ImportDataOperator,
    ListDatasetsOperator,
    UpdateDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_vertex_ai_dataset_ops"
LOCATION_REGION = "us-central1"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
TIME_SERIES_DATASET = {
    "display_name": f"time-series-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.time_series,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [
                        f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/forecast-dataset.csv"
                    ]
                }
            }
        },
        Value(),
    ),
}
IMAGE_DATASET = {
    "display_name": f"image-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.image,
    "metadata": Value(string_value="image-dataset"),
}
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [
                        f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/tabular-dataset.csv"
                    ]
                }
            }
        },
        Value(),
    ),
}
TEXT_DATASET = {
    "display_name": f"text-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="text-dataset"),
}
VIDEO_DATASET = {
    "display_name": f"video-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.video,
    "metadata": Value(string_value="video-dataset"),
}
TEST_EXPORT_CONFIG = {
    "gcs_destination": {
        "output_uri_prefix": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/exports"
    }
}
TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {"test-labels-name": "test-labels-value"},
        "import_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/ioformat/image_bounding_box_io_format_1.0.0.yaml",
        "gcs_source": {"uris": ["gs://cloud-samples-data/vision/salads.csv"]},
    }
]
DATASET_TO_UPDATE = {"display_name": "test-name"}
TEST_UPDATE_MASK = {"paths": ["displayName"]}
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
    description="This Airflow DAG demonstrates various operations on Vertex AI Datasets, including creation, deletion, listing, getting, updating, importing, and exporting.",
    tags=["demo", "google_cloud", "vertex_ai"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_datasets_files = GCSSynchronizeBucketsOperator(
        task_id="move_datasets_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/datasets",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    create_image_dataset_job = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    create_tabular_dataset_job = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    create_text_dataset_job = CreateDatasetOperator(
        task_id="text_dataset",
        dataset=TEXT_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    create_video_dataset_job = CreateDatasetOperator(
        task_id="video_dataset",
        dataset=VIDEO_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    create_time_series_dataset_job = CreateDatasetOperator(
        task_id="time_series_dataset",
        dataset=TIME_SERIES_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_dataset_job = DeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=create_text_dataset_job.output["dataset_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    get_dataset = GetDatasetOperator(
        task_id="get_dataset",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        dataset_id=create_tabular_dataset_job.output["dataset_id"],
    )
    export_data_job = ExportDataOperator(
        task_id="export_data",
        dataset_id=create_image_dataset_job.output["dataset_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        export_config=TEST_EXPORT_CONFIG,
    )
    import_data_job = ImportDataOperator(
        task_id="import_data",
        dataset_id=create_image_dataset_job.output["dataset_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        import_configs=TEST_IMPORT_CONFIG,
    )
    list_dataset_job = ListDatasetsOperator(
        task_id="list_dataset", region=LOCATION_REGION, project_id=PROJECT_ID
    )
    update_dataset_job = UpdateDatasetOperator(
        task_id="update_dataset",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        dataset_id=create_video_dataset_job.output["dataset_id"],
        dataset=DATASET_TO_UPDATE,
        update_mask=TEST_UPDATE_MASK,
    )
    delete_time_series_dataset_job = DeleteDatasetOperator(
        task_id="delete_time_series_dataset",
        dataset_id=create_time_series_dataset_job.output["dataset_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_tabular_dataset_job = DeleteDatasetOperator(
        task_id="delete_tabular_dataset",
        dataset_id=create_tabular_dataset_job.output["dataset_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_image_dataset_job = DeleteDatasetOperator(
        task_id="delete_image_dataset",
        dataset_id=create_image_dataset_job.output["dataset_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_video_dataset_job = DeleteDatasetOperator(
        task_id="delete_video_dataset",
        dataset_id=create_video_dataset_job.output["dataset_id"],
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
        create_bucket
        >> move_datasets_files
        >> [
            create_time_series_dataset_job >> delete_time_series_dataset_job,
            create_text_dataset_job >> delete_dataset_job,
            create_tabular_dataset_job >> get_dataset >> delete_tabular_dataset_job,
            create_image_dataset_job
            >> import_data_job
            >> export_data_job
            >> delete_image_dataset_job,
            create_video_dataset_job >> update_dataset_job >> delete_video_dataset_job,
            list_dataset_job,
        ]
        >> delete_bucket
    )
