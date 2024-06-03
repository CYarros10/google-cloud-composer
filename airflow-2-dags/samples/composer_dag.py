"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from __future__ import annotations

from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerUpdateEnvironmentOperator,
)

ENV_ID = "your-env"
PROJECT_ID = "your-project"
REGION = "your-region"

# [START howto_operator_composer_simple_environment]
UPDATE_ENVIRONMENT_CONFIG = {
    "config": {
        "workloads_config": {
            "worker": {
                "min_count": 1
            }
        }
    }
}
UPDATE_MASK = {"paths": ["config.workloads_config.worker.min_count",]}

with DAG(
    "composer_dag",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "composer"],
) as dag:
   

    update_env = CloudComposerUpdateEnvironmentOperator(
        task_id="update_env",
        project_id=PROJECT_ID,
        region=REGION,
        environment_id=ENV_ID,
        update_mask=UPDATE_MASK,
        environment=UPDATE_ENVIRONMENT_CONFIG,
    )

