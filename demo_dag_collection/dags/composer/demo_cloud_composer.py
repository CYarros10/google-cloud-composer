from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_composer import (
    CloudComposerCreateEnvironmentOperator,
    CloudComposerDeleteEnvironmentOperator,
    CloudComposerGetEnvironmentOperator,
    CloudComposerListEnvironmentsOperator,
    CloudComposerListImageVersionsOperator,
    CloudComposerUpdateEnvironmentOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_composer"
LOCATION_REGION = "us-central1"
ENVIRONMENT_ID = f"test-{DAG_ID}-{ENV_ID}".replace("_", "-")
ENVIRONMENT_ID_ASYNC = f"test-deferrable-{DAG_ID}-{ENV_ID}".replace("_", "-")
ENVIRONMENT = {
    "config": {"software_config": {"image_version": "composer-2.5.0-airflow-2.5.3"}}
}
UPDATED_ENVIRONMENT = {"labels": {"label": "testing"}}
UPDATE_MASK = {"paths": ["labels.label"]}
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=55),
    },
    description="This Airflow DAG demonstrates the ability to manage Cloud Composer environments using various operators.",
    tags=["demo", "google_cloud", "composer"],
) as dag:
    image_versions = CloudComposerListImageVersionsOperator(
        task_id="image_versions", project_id=PROJECT_ID, region=LOCATION_REGION
    )
    create_env = CloudComposerCreateEnvironmentOperator(
        task_id="create_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID,
        environment=ENVIRONMENT,
    )
    defer_create_env = CloudComposerCreateEnvironmentOperator(
        task_id="defer_create_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        environment=ENVIRONMENT,
        deferrable=True,
    )
    list_envs = CloudComposerListEnvironmentsOperator(
        task_id="list_envs", project_id=PROJECT_ID, region=LOCATION_REGION
    )
    get_env = CloudComposerGetEnvironmentOperator(
        task_id="get_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID,
    )
    update_env = CloudComposerUpdateEnvironmentOperator(
        task_id="update_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID,
        update_mask=UPDATE_MASK,
        environment=UPDATED_ENVIRONMENT,
    )
    defer_update_env = CloudComposerUpdateEnvironmentOperator(
        task_id="defer_update_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        update_mask=UPDATE_MASK,
        environment=UPDATED_ENVIRONMENT,
        deferrable=True,
    )
    delete_env = CloudComposerDeleteEnvironmentOperator(
        task_id="delete_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID,
    )
    delete_env.trigger_rule = TriggerRule.ALL_DONE
    defer_delete_env = CloudComposerDeleteEnvironmentOperator(
        task_id="defer_delete_env",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        environment_id=ENVIRONMENT_ID_ASYNC,
        deferrable=True,
    )
    defer_delete_env.trigger_rule = TriggerRule.ALL_DONE
    chain(
        image_versions,
        [create_env, defer_create_env],
        list_envs,
        get_env,
        [update_env, defer_update_env],
        [delete_env, defer_delete_env],
    )
