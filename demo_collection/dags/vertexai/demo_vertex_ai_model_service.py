"""
Example Airflow DAG for Google Vertex AI service testing Model Service operations.
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
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.model_service import (
    AddVersionAliasesOnModelOperator,
    DeleteModelOperator,
    DeleteModelVersionOperator,
    DeleteVersionAliasesOnModelOperator,
    ExportModelOperator,
    GetModelOperator,
    ListModelsOperator,
    ListModelVersionsOperator,
    SetDefaultVersionOnModelOperator,
    UploadModelOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_vertex_ai_model_service_ops"
LOCATION_REGION = "us-central1"
TRAIN_DISPLAY_NAME = f"train-housing-custom-{ENV_ID}"
MODEL_DISPLAY_NAME = f"custom-housing-model-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
STAGING_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"
DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/california_housing_train.csv"
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [
                        f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/{DATA_SAMPLE_GCS_OBJECT_NAME}"
                    ]
                }
            }
        },
        Value(),
    ),
}
CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
LOCAL_TRAINING_SCRIPT_PATH = "california_housing_training_script.py"
MODEL_OUTPUT_CONFIG = {
    "artifact_destination": {"output_uri_prefix": STAGING_BUCKET},
    "export_format_id": "custom-trained",
}
MODEL_SERVING_CONTAINER_URI = "gcr.io/cloud-aiplatform/prediction/tf2-cpu.2-2:latest"
MODEL_OBJ = {
    "display_name": f"model-{ENV_ID}",
    "artifact_uri": "{{ti.xcom_pull('custom_task')['artifactUri']}}",
    "container_spec": {
        "image_uri": MODEL_SERVING_CONTAINER_URI,
        "command": [],
        "args": [],
        "env": [],
        "ports": [],
        "predict_route": "",
        "health_route": "",
    },
}
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
    description="This Airflow DAG demonstrates how to use the Vertex AI Model Service to manage models and versions.",
    tags=["demo", "google_cloud", "vertex_ai"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_data_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/california-housing-data",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    download_training_script_file = GCSToLocalFilesystemOperator(
        task_id="download_training_script_file",
        object_name="vertex-ai/california_housing_training_script.py",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        filename=LOCAL_TRAINING_SCRIPT_PATH,
    )
    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]
    create_custom_training_job = CreateCustomTrainingJobOperator(
        task_id="custom_task",
        staging_bucket=f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}",
        display_name=TRAIN_DISPLAY_NAME,
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        dataset_id=tabular_dataset_id,
        replica_count=1,
        model_display_name=MODEL_DISPLAY_NAME,
        sync=False,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    model_id_v1 = create_custom_training_job.output["model_id"]
    create_custom_training_job_v2 = CreateCustomTrainingJobOperator(
        task_id="custom_task_v2",
        staging_bucket=f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}",
        display_name=TRAIN_DISPLAY_NAME,
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        parent_model=model_id_v1,
        dataset_id=tabular_dataset_id,
        replica_count=1,
        model_display_name=MODEL_DISPLAY_NAME,
        sync=False,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    model_id_v2 = create_custom_training_job_v2.output["model_id"]
    get_model = GetModelOperator(
        task_id="get_model",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        model_id=model_id_v1,
    )
    list_model_versions = ListModelVersionsOperator(
        task_id="list_model_versions",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        model_id=model_id_v1,
    )
    set_default_version = SetDefaultVersionOnModelOperator(
        task_id="set_default_version",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        model_id=model_id_v2,
    )
    add_version_alias = AddVersionAliasesOnModelOperator(
        task_id="add_version_alias",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        version_aliases=["new-version", "beta"],
        model_id=model_id_v2,
    )
    upload_model = UploadModelOperator(
        task_id="upload_model",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        model=MODEL_OBJ,
    )
    export_model = ExportModelOperator(
        task_id="export_model",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        model_id=upload_model.output["model_id"],
        output_config=MODEL_OUTPUT_CONFIG,
    )
    delete_model = DeleteModelOperator(
        task_id="delete_model",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        model_id=upload_model.output["model_id"],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    list_models = ListModelsOperator(
        task_id="list_models", region=LOCATION_REGION, project_id=PROJECT_ID
    )
    delete_version_alias = DeleteVersionAliasesOnModelOperator(
        task_id="delete_version_alias",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        version_aliases=["new-version"],
        model_id=model_id_v2,
    )
    delete_model_version = DeleteModelVersionOperator(
        task_id="delete_model_version",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        model_id=model_id_v1,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='custom_task', key='training_id') }}",
        custom_job_id="{{ task_instance.xcom_pull(task_ids='custom_task', key='custom_job_id') }}",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_tabular_dataset = DeleteDatasetOperator(
        task_id="delete_tabular_dataset",
        dataset_id=tabular_dataset_id,
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
        >> move_data_files
        >> download_training_script_file
        >> create_tabular_dataset
        >> create_custom_training_job
        >> create_custom_training_job_v2
        >> get_model
        >> list_model_versions
        >> set_default_version
        >> add_version_alias
        >> upload_model
        >> export_model
        >> delete_model
        >> list_models
        >> delete_version_alias
        >> delete_model_version
        >> delete_custom_training_job
        >> delete_tabular_dataset
        >> delete_bucket
    )
