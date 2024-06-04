"""
Example Airflow DAG that creates, update, get and delete trigger for Data Loss Prevention actions.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateJobTriggerOperator,
    CloudDLPDeleteJobTriggerOperator,
    CloudDLPGetDLPJobTriggerOperator,
    CloudDLPListJobTriggersOperator,
    CloudDLPUpdateJobTriggerOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dlp_job_trigger"
ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
JOB_TRIGGER = {
    "inspect_job": {
        "storage_config": {
            "datastore_options": {
                "partition_id": {"project_id": PROJECT_ID},
                "kind": {"name": "test"},
            }
        }
    },
    "triggers": [
        {"schedule": {"recurrence_period_duration": {"seconds": 60 * 60 * 24}}}
    ],
    "status": "HEALTHY",
}
TRIGGER_ID = f"trigger_{ENV_ID}"
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
    description="A DAG that demonstrates Data Loss Prevention trigger creation, listing, update, and deletion for the Data Loss Prevention service.",
    tags=["demo", "google_cloud", "dlp"],
) as dag:
    create_trigger = CloudDLPCreateJobTriggerOperator(
        project_id=PROJECT_ID,
        job_trigger=JOB_TRIGGER,
        trigger_id=TRIGGER_ID,
        task_id="create_trigger",
    )
    list_triggers = CloudDLPListJobTriggersOperator(
        task_id="list_triggers", project_id=PROJECT_ID
    )
    get_trigger = CloudDLPGetDLPJobTriggerOperator(
        task_id="get_trigger", project_id=PROJECT_ID, job_trigger_id=TRIGGER_ID
    )
    JOB_TRIGGER["triggers"] = [
        {"schedule": {"recurrence_period_duration": {"seconds": 2 * 60 * 60 * 24}}}
    ]
    update_trigger = CloudDLPUpdateJobTriggerOperator(
        project_id=PROJECT_ID,
        job_trigger_id=TRIGGER_ID,
        job_trigger=JOB_TRIGGER,
        task_id="update_info_type",
    )
    delete_trigger = CloudDLPDeleteJobTriggerOperator(
        project_id=PROJECT_ID, job_trigger_id=TRIGGER_ID, task_id="delete_info_type"
    )
    delete_trigger.trigger_rule = TriggerRule.ALL_DONE
    create_trigger >> list_triggers >> get_trigger >> update_trigger >> delete_trigger
