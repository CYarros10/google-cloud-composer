"""
Example Airflow DAG for Google Vertex AI Generative Model prompting.
"""

from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    PromptLanguageModelOperator,
    PromptMultimodalModelOperator,
)

PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"
DAG_ID = "demo_vertex_ai_generative_model"
with models.DAG(
    dag_id=DAG_ID,
    description="This Airflow DAG demonstrates prompting Google Vertex AI Generative Models, including text-based and multimodal models. \n",
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
    tags=["demo", "google_cloud", "vertex_ai", "generative_ai"],
) as dag:
    prompt_language_model_task = PromptLanguageModelOperator(
        task_id="prompt_language_model_task",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        prompt="Give me a sample itinerary for a trip to New Zealand.",
        pretrained_model="text-bison",
    )
    prompt_multimodal_model_task = PromptMultimodalModelOperator(
        task_id="generative_model_task",
        project_id=PROJECT_ID,
        location=LOCATION_REGION,
        prompt="Give me a sample itinerary for a trip to Australia.",
        pretrained_model="gemini-pro",
    )
