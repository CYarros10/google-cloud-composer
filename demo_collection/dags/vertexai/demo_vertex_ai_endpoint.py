"""
Example Airflow DAG for Google Vertex AI service testing Endpoint Service operations.
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
from airflow.providers.google.cloud.operators.vertex_ai.endpoint_service import (
    CreateEndpointOperator,
    DeleteEndpointOperator,
    DeployModelOperator,
    ListEndpointsOperator,
    UndeployModelOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_vertex_ai_endpoint_service_ops"
LOCATION_REGION = "us-central1"
IMAGE_DISPLAY_NAME = f"auto-ml-image-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-image-model-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
IMAGE_DATASET = {
    "display_name": f"image-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.image,
    "metadata": Value(string_value="image-dataset"),
}
IMAGE_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.image.single_label_classification,
        "gcs_source": {
            "uris": [f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/image-dataset.csv"]
        },
    }
]
ENDPOINT_CONF = {"display_name": f"endpoint_test_{ENV_ID}"}
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
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
    description="This Airflow DAG tests Google Vertex AI Endpoint Service operations.",
    tags=["demo", "google_cloud", "vertex_ai"],
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
        source_object="vertex-ai/datasets",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
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
    DEPLOYED_MODEL = {
        "model": "{{ti.xcom_pull('auto_ml_image_task')['name']}}",
        "display_name": f"temp_endpoint_test_{ENV_ID}",
        "automatic_resources": {"min_replica_count": 1, "max_replica_count": 1},
    }
    create_endpoint = CreateEndpointOperator(
        task_id="create_endpoint",
        endpoint=ENDPOINT_CONF,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_endpoint = DeleteEndpointOperator(
        task_id="delete_endpoint",
        endpoint_id=create_endpoint.output["endpoint_id"],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    list_endpoints = ListEndpointsOperator(
        task_id="list_endpoints", region=LOCATION_REGION, project_id=PROJECT_ID
    )
    deploy_model = DeployModelOperator(
        task_id="deploy_model",
        endpoint_id=create_endpoint.output["endpoint_id"],
        deployed_model=DEPLOYED_MODEL,
        traffic_split={"0": 100},
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    undeploy_model = UndeployModelOperator(
        task_id="undeploy_model",
        endpoint_id=create_endpoint.output["endpoint_id"],
        deployed_model_id=deploy_model.output["deployed_model_id"],
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
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> move_dataset_file, create_image_dataset]
        >> import_image_dataset
        >> create_auto_ml_image_training_job
        >> create_endpoint
        >> deploy_model
        >> undeploy_model
        >> delete_endpoint
        >> list_endpoints
        >> delete_auto_ml_image_training_job
        >> delete_image_dataset
        >> delete_bucket
    )
