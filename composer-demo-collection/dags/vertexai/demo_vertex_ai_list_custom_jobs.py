"""
Example Airflow DAG for Google Vertex AI service testing Custom Jobs operations.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    ListCustomTrainingJobOperator,
)

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_vertex_ai_custom_job_ops"
LOCATION_REGION = "us-central1"
with DAG(
    f"{DAG_ID}_list_custom_job",
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
    description="This Airflow DAG lists custom training jobs in a Vertex AI project.",
    tags=["demo", "google_cloud", "vertex_ai"],
) as dag:
    list_custom_training_job = ListCustomTrainingJobOperator(
        task_id="list_custom_training_job",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
