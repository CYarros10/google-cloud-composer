"""
Example Airflow DAG for Google Vertex AI service testing Pipeline Job operations.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSListObjectsOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    DeletePipelineJobOperator,
    GetPipelineJobOperator,
    ListPipelineJobOperator,
    RunPipelineJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_vertex_ai_pipeline_job_ops"
LOCATION_REGION = "us-central1"
DISPLAY_NAME = f"pipeline-job-{ENV_ID}"
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
TEMPLATE_PATH = "https://us-kfp.pkg.dev/ml-pipeline/google-cloud-registry/automl-tabular/sha256:85e4218fc6604ee82353c9d2ebba20289eb1b71930798c0bb8ce32d8a10de146"
OUTPUT_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"
PARAMETER_VALUES = {
    "train_budget_milli_node_hours": 2000,
    "optimization_objective": "minimize-log-loss",
    "project": PROJECT_ID,
    "location": LOCATION_REGION,
    "root_dir": OUTPUT_BUCKET,
    "target_column": "Adopted",
    "training_fraction": 0.8,
    "validation_fraction": 0.1,
    "test_fraction": 0.1,
    "prediction_type": "classification",
    "data_source_csv_filenames": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/tabular-dataset.csv",
    "transformations": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/column_transformations.json",
}
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
    description="This Airflow DAG demonstrates Vertex AI Pipeline Job operations.",
    tags=["demo", "google_cloud", "vertex_ai"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_pipeline_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/pipeline",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    run_pipeline_job = RunPipelineJobOperator(
        task_id="run_pipeline_job",
        display_name=DISPLAY_NAME,
        template_path=TEMPLATE_PATH,
        parameter_values=PARAMETER_VALUES,
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    get_pipeline_job = GetPipelineJobOperator(
        task_id="get_pipeline_job",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        pipeline_job_id="{{ task_instance.xcom_pull(task_ids='run_pipeline_job', key='pipeline_job_id') }}",
    )
    delete_pipeline_job = DeletePipelineJobOperator(
        task_id="delete_pipeline_job",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        pipeline_job_id="{{ task_instance.xcom_pull(task_ids='run_pipeline_job', key='pipeline_job_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    list_pipeline_job = ListPipelineJobOperator(
        task_id="list_pipeline_job", region=LOCATION_REGION, project_id=PROJECT_ID
    )
    list_buckets = GCSListObjectsOperator(
        task_id="list_buckets", bucket=DATA_SAMPLE_GCS_BUCKET_NAME
    )
    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        objects=list_buckets.output,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> move_pipeline_files
        >> run_pipeline_job
        >> get_pipeline_job
        >> delete_pipeline_job
        >> list_pipeline_job
        >> list_buckets
        >> delete_files
        >> delete_bucket
    )
