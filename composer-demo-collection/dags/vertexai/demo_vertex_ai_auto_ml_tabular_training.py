"""
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
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
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLTabularTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_vertex_ai_auto_ml_ops"
LOCATION_REGION = "us-central1"
TABULAR_DISPLAY_NAME = f"auto-ml-tabular-{ENV_ID}"
MODEL_DISPLAY_NAME = "adopted-prediction-model"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
TABULAR_GCS_BUCKET_NAME = f"bucket_tabular_{DAG_ID}_{ENV_ID}".replace("_", "-")
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [
                        f"gs://{TABULAR_GCS_BUCKET_NAME}/vertex-ai/tabular-dataset.csv"
                    ]
                }
            }
        },
        Value(),
    ),
}
COLUMN_TRANSFORMATIONS = [
    {"categorical": {"column_name": "Type"}},
    {"numeric": {"column_name": "Age"}},
    {"categorical": {"column_name": "Breed1"}},
    {"categorical": {"column_name": "Color1"}},
    {"categorical": {"column_name": "Color2"}},
    {"categorical": {"column_name": "MaturitySize"}},
    {"categorical": {"column_name": "FurLength"}},
    {"categorical": {"column_name": "Vaccinated"}},
    {"categorical": {"column_name": "Sterilized"}},
    {"categorical": {"column_name": "Health"}},
    {"numeric": {"column_name": "Fee"}},
    {"numeric": {"column_name": "PhotoAmt"}},
]
with DAG(
    f"{DAG_ID}_tabular_training_job",
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
    description="This Airflow DAG creates a Vertex AI AutoML tabular training job.",
    tags=["demo", "google_cloud", "vertex_ai", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=TABULAR_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/datasets",
        destination_bucket=TABULAR_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]
    create_auto_ml_tabular_training_job = CreateAutoMLTabularTrainingJobOperator(
        task_id="auto_ml_tabular_task",
        display_name=TABULAR_DISPLAY_NAME,
        optimization_prediction_type="classification",
        column_transformations=COLUMN_TRANSFORMATIONS,
        dataset_id=tabular_dataset_id,
        target_column="Adopted",
        training_fraction_split=0.8,
        validation_fraction_split=0.1,
        test_fraction_split=0.1,
        model_display_name=MODEL_DISPLAY_NAME,
        disable_early_stopping=False,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_auto_ml_tabular_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_tabular_task', key='training_id') }}",
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
        bucket_name=TABULAR_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> move_dataset_file
        >> create_tabular_dataset
        >> create_auto_ml_tabular_training_job
        >> delete_auto_ml_tabular_training_job
        >> delete_tabular_dataset
        >> delete_bucket
    )
