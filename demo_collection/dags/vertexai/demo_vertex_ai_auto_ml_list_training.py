"""
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    ListAutoMLTrainingJobOperator,
)

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_vertex_ai_auto_ml_ops"
LOCATION_REGION = "us-central1"
with DAG(
    f"{DAG_ID}_list_training_job",
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
    description="This Airflow DAG lists AutoML training jobs in a Google Vertex AI project.",
    tags=["demo", "google_cloud", "vertex_ai", "automl"],
) as dag:
    list_auto_ml_training_job = ListAutoMLTrainingJobOperator(
        task_id="list_auto_ml_training_job",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
