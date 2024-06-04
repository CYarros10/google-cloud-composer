"""
Example Airflow DAG that displays interactions with Google Cloud Build.
"""

from datetime import datetime, timedelta
from typing import Any, cast
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator,
    CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildTriggersOperator,
    CloudBuildRunBuildTriggerOperator,
    CloudBuildUpdateBuildTriggerOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_cloud_build_trigger"
GCP_SOURCE_REPOSITORY_NAME = "test-cloud-build-repo"
TRIGGER_NAME = f"cloud-build-trigger-{ENV_ID}"
create_build_trigger_body = {
    "name": TRIGGER_NAME,
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "main",
    },
    "filename": "example_cloud_build.yaml",
}
update_build_trigger_body = {
    "name": TRIGGER_NAME,
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": GCP_SOURCE_REPOSITORY_NAME,
        "branch_name": "master",
    },
    "filename": "example_cloud_build.yaml",
}
create_build_from_repo_body: dict[str, Any] = {
    "source": {
        "repo_source": {
            "repo_name": GCP_SOURCE_REPOSITORY_NAME,
            "branch_name": "master",
        }
    },
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world"]}],
}
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
    description="This Airflow DAG demonstrates interactions with Google Cloud Build API. \n",
    tags=["demo", "google_cloud", "cloud_build"],
) as dag:
    create_build_trigger = CloudBuildCreateBuildTriggerOperator(
        task_id="create_build_trigger",
        project_id=PROJECT_ID,
        trigger=create_build_trigger_body,
    )
    build_trigger_id = cast(str, XComArg(create_build_trigger, key="id"))
    run_build_trigger = CloudBuildRunBuildTriggerOperator(
        task_id="run_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        source=create_build_from_repo_body["source"]["repo_source"],
    )
    update_build_trigger = CloudBuildUpdateBuildTriggerOperator(
        task_id="update_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        trigger=update_build_trigger_body,
    )
    get_build_trigger = CloudBuildGetBuildTriggerOperator(
        task_id="get_build_trigger", project_id=PROJECT_ID, trigger_id=build_trigger_id
    )
    delete_build_trigger = CloudBuildDeleteBuildTriggerOperator(
        task_id="delete_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
    )
    delete_build_trigger.trigger_rule = TriggerRule.ALL_DONE
    list_build_triggers = CloudBuildListBuildTriggersOperator(
        task_id="list_build_triggers",
        project_id=PROJECT_ID,
        location="global",
        page_size=5,
    )
    chain(
        create_build_trigger,
        run_build_trigger,
        update_build_trigger,
        get_build_trigger,
        delete_build_trigger,
        list_build_triggers,
    )
