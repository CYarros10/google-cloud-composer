"""
Example Airflow DAG for Google Vertex AI service testing Batch Prediction operations.
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
    CreateAutoMLForecastingTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (
    CreateBatchPredictionJobOperator,
    DeleteBatchPredictionJobOperator,
    ListBatchPredictionJobsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_vertex_ai_batch_prediction_ops"
LOCATION_REGION = "us-central1"
FORECAST_DISPLAY_NAME = f"auto-ml-forecasting-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-forecasting-model-{ENV_ID}"
JOB_DISPLAY_NAME = f"batch_prediction_job_test_{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/forecast-dataset.csv"
FORECAST_DATASET = {
    "display_name": f"forecast-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.time_series,
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
TEST_TIME_COLUMN = "date"
TEST_TIME_SERIES_IDENTIFIER_COLUMN = "store_name"
TEST_TARGET_COLUMN = "sale_dollars"
COLUMN_SPECS = {
    TEST_TIME_COLUMN: "timestamp",
    TEST_TARGET_COLUMN: "numeric",
    "city": "categorical",
    "zip_code": "categorical",
    "county": "categorical",
}
BIGQUERY_SOURCE = (
    f"bq://{PROJECT_ID}.test_iowa_liquor_sales_forecasting_us.2021_sales_predict"
)
GCS_DESTINATION_PREFIX = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/output"
MODEL_PARAMETERS: dict[str, str] = {}
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
    description="This Airflow DAG creates a Vertex AI batch prediction job using an AutoML forecasting model.",
    tags=["demo", "google_cloud", "vertex_ai", "automl"],
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
    create_forecast_dataset = CreateDatasetOperator(
        task_id="forecast_dataset",
        dataset=FORECAST_DATASET,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    create_auto_ml_forecasting_training_job = (
        CreateAutoMLForecastingTrainingJobOperator(
            task_id="auto_ml_forecasting_task",
            display_name=FORECAST_DISPLAY_NAME,
            optimization_objective="minimize-rmse",
            column_specs=COLUMN_SPECS,
            dataset_id=create_forecast_dataset.output["dataset_id"],
            target_column=TEST_TARGET_COLUMN,
            time_column=TEST_TIME_COLUMN,
            time_series_identifier_column=TEST_TIME_SERIES_IDENTIFIER_COLUMN,
            available_at_forecast_columns=[TEST_TIME_COLUMN],
            unavailable_at_forecast_columns=[TEST_TARGET_COLUMN],
            time_series_attribute_columns=["city", "zip_code", "county"],
            forecast_horizon=30,
            context_window=30,
            data_granularity_unit="day",
            data_granularity_count=1,
            weight_column=None,
            budget_milli_node_hours=1000,
            model_display_name=MODEL_DISPLAY_NAME,
            predefined_split_column_name=None,
            region=LOCATION_REGION,
            project_id=PROJECT_ID,
        )
    )
    create_batch_prediction_job = CreateBatchPredictionJobOperator(
        task_id="create_batch_prediction_job",
        job_display_name=JOB_DISPLAY_NAME,
        model_name="{{ti.xcom_pull('auto_ml_forecasting_task')['name']}}",
        predictions_format="csv",
        bigquery_source=BIGQUERY_SOURCE,
        gcs_destination_prefix=GCS_DESTINATION_PREFIX,
        model_parameters=MODEL_PARAMETERS,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    list_batch_prediction_job = ListBatchPredictionJobsOperator(
        task_id="list_batch_prediction_jobs",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_batch_prediction_job = DeleteBatchPredictionJobOperator(
        task_id="delete_batch_prediction_job",
        batch_prediction_job_id=create_batch_prediction_job.output[
            "batch_prediction_job_id"
        ],
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_auto_ml_forecasting_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_forecasting_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_forecasting_task', key='training_id') }}",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_forecast_dataset = DeleteDatasetOperator(
        task_id="delete_forecast_dataset",
        dataset_id=create_forecast_dataset.output["dataset_id"],
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
        >> move_dataset_file
        >> create_forecast_dataset
        >> create_auto_ml_forecasting_training_job
        >> create_batch_prediction_job
        >> list_batch_prediction_job
        >> delete_batch_prediction_job
        >> delete_auto_ml_forecasting_training_job
        >> delete_forecast_dataset
        >> delete_bucket
    )
