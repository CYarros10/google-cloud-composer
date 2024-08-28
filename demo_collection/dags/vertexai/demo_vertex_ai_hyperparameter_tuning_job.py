"""
Example Airflow DAG for Google Vertex AI service testing Hyperparameter Tuning Job operations.
"""

from datetime import datetime, timedelta
from google.cloud import aiplatform
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job import (
    CreateHyperparameterTuningJobOperator,
    DeleteHyperparameterTuningJobOperator,
    GetHyperparameterTuningJobOperator,
    ListHyperparameterTuningJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_vertex_ai_hyperparameter_tuning_job_ops"
LOCATION_REGION = "us-central1"
DISPLAY_NAME = f"hyperparameter-tuning-job-{ENV_ID}"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_hyperparameter_tuning_job_{ENV_ID}"
STAGING_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
WORKER_POOL_SPECS = [
    {
        "machine_spec": {
            "machine_type": MACHINE_TYPE,
            "accelerator_type": ACCELERATOR_TYPE,
            "accelerator_count": ACCELERATOR_COUNT,
        },
        "replica_count": REPLICA_COUNT,
        "container_spec": {
            "image_uri": "us-docker.pkg.dev/composer-256318/horse-human/horse-human-image:latest"
        },
    }
]
PARAM_SPECS = {
    "learning_rate": aiplatform.hyperparameter_tuning.DoubleParameterSpec(
        min=0.01, max=1, scale="log"
    ),
    "momentum": aiplatform.hyperparameter_tuning.DoubleParameterSpec(
        min=0, max=1, scale="linear"
    ),
    "num_neurons": aiplatform.hyperparameter_tuning.DiscreteParameterSpec(
        values=[64, 128, 512], scale="linear"
    ),
}
METRIC_SPEC = {"accuracy": "maximize"}
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
    description="This Airflow DAG creates, gets, lists, and deletes Vertex AI Hyperparameter Tuning Jobs.",
    tags=["demo", "google_cloud", "vertex_ai"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    create_hyperparameter_tuning_job = CreateHyperparameterTuningJobOperator(
        task_id="create_hyperparameter_tuning_job",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        worker_pool_specs=WORKER_POOL_SPECS,
        sync=False,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        parameter_spec=PARAM_SPECS,
        metric_spec=METRIC_SPEC,
        max_trial_count=15,
        parallel_trial_count=3,
    )
    create_hyperparameter_tuning_job_def = CreateHyperparameterTuningJobOperator(
        task_id="create_hyperparameter_tuning_job_def",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        worker_pool_specs=WORKER_POOL_SPECS,
        sync=False,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        parameter_spec=PARAM_SPECS,
        metric_spec=METRIC_SPEC,
        max_trial_count=15,
        parallel_trial_count=3,
        deferrable=True,
    )
    get_hyperparameter_tuning_job = GetHyperparameterTuningJobOperator(
        task_id="get_hyperparameter_tuning_job",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        hyperparameter_tuning_job_id="{{ task_instance.xcom_pull(task_ids='create_hyperparameter_tuning_job', key='hyperparameter_tuning_job_id') }}",
    )
    delete_hyperparameter_tuning_job = DeleteHyperparameterTuningJobOperator(
        task_id="delete_hyperparameter_tuning_job",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        hyperparameter_tuning_job_id="{{ task_instance.xcom_pull(task_ids='create_hyperparameter_tuning_job', key='hyperparameter_tuning_job_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_hyperparameter_tuning_job_def = DeleteHyperparameterTuningJobOperator(
        task_id="delete_hyperparameter_tuning_job_def",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        hyperparameter_tuning_job_id="{{ task_instance.xcom_pull(task_ids='create_hyperparameter_tuning_job_def', key='hyperparameter_tuning_job_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    list_hyperparameter_tuning_job = ListHyperparameterTuningJobOperator(
        task_id="list_hyperparameter_tuning_job",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> [create_hyperparameter_tuning_job, create_hyperparameter_tuning_job_def]
        >> get_hyperparameter_tuning_job
        >> [delete_hyperparameter_tuning_job, delete_hyperparameter_tuning_job_def]
        >> list_hyperparameter_tuning_job
        >> delete_bucket
    )
