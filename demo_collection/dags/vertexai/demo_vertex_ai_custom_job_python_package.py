"""
Example Airflow DAG for Google Vertex AI service testing Custom Jobs operations.
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
    CreateCustomPythonPackageTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_vertex_ai_custom_job_ops"
LOCATION_REGION = "us-central1"
PACKAGE_DISPLAY_NAME = f"train-housing-py-package-{ENV_ID}"
MODEL_DISPLAY_NAME = f"py-package-housing-model-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
CUSTOM_PYTHON_GCS_BUCKET_NAME = f"bucket_python_{DAG_ID}_{ENV_ID}".replace("_", "-")
DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/california_housing_train.csv"


def TABULAR_DATASET(bucket_name):
    return {
        "display_name": f"tabular-dataset-{ENV_ID}",
        "metadata_schema_uri": schema.dataset.metadata.tabular,
        "metadata": ParseDict(
            {
                "input_config": {
                    "gcs_source": {
                        "uri": [f"gs://{bucket_name}/{DATA_SAMPLE_GCS_OBJECT_NAME}"]
                    }
                }
            },
            Value(),
        ),
    }


CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
MODEL_SERVING_CONTAINER_URI = "gcr.io/cloud-aiplatform/prediction/tf2-cpu.2-2:latest"
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
TRAINING_FRACTION_SPLIT = 0.7
TEST_FRACTION_SPLIT = 0.15
VALIDATION_FRACTION_SPLIT = 0.15
PYTHON_PACKAGE_GCS_URI = (
    f"gs://{CUSTOM_PYTHON_GCS_BUCKET_NAME}/vertex-ai/custom_trainer_script-0.1.tar"
)
PYTHON_MODULE_NAME = "aiplatform_custom_trainer_script.task"
with DAG(
    f"{DAG_ID}_python_package",
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
    description="This DAG creates a custom Python package training job in Vertex AI.",
    tags=["demo", "google_cloud", "vertex_ai", "python"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=CUSTOM_PYTHON_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_data_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/california-housing-data",
        destination_bucket=CUSTOM_PYTHON_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET(CUSTOM_PYTHON_GCS_BUCKET_NAME),
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]
    create_custom_python_package_training_job = (
        CreateCustomPythonPackageTrainingJobOperator(
            task_id="python_package_task",
            staging_bucket=f"gs://{CUSTOM_PYTHON_GCS_BUCKET_NAME}",
            display_name=PACKAGE_DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
            dataset_id=tabular_dataset_id,
            model_display_name=MODEL_DISPLAY_NAME,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=LOCATION_REGION,
            project_id=PROJECT_ID,
        )
    )
    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='python_package_task', key='training_id') }}",
        custom_job_id="{{ task_instance.xcom_pull(task_ids='python_package_task', key='custom_job_id') }}",
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
        bucket_name=CUSTOM_PYTHON_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> move_data_files
        >> create_tabular_dataset
        >> create_custom_python_package_training_job
        >> delete_custom_training_job
        >> delete_tabular_dataset
        >> delete_bucket
    )
