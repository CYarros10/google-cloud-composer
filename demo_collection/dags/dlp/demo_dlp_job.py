"""
Example Airflow DAG that creates, update, get and delete trigger for Data Loss Prevention actions.
"""

from datetime import datetime, timedelta
from google.cloud.dlp_v2.types import InspectConfig, InspectJobConfig
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCancelDLPJobOperator,
    CloudDLPCreateDLPJobOperator,
    CloudDLPDeleteDLPJobOperator,
    CloudDLPGetDLPJobOperator,
    CloudDLPListDLPJobsOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dlp_job"
ENV_ID = "composer"
PROJECT_ID = "your-project"
JOB_ID = f"dlp_job_{ENV_ID}"
INSPECT_CONFIG = InspectConfig(
    info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}]
)
INSPECT_JOB = InspectJobConfig(
    inspect_config=INSPECT_CONFIG,
    storage_config={
        "datastore_options": {
            "partition_id": {"project_id": PROJECT_ID},
            "kind": {"name": "test"},
        }
    },
)
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
    description="Creates, updates, gets, and deletes Data Loss Prevention (DLP) actions.",
    tags=["demo", "google_cloud", "dlp"],
) as dag:
    create_job = CloudDLPCreateDLPJobOperator(
        task_id="create_job",
        project_id=PROJECT_ID,
        inspect_job=INSPECT_JOB,
        job_id=JOB_ID,
    )
    list_jobs = CloudDLPListDLPJobsOperator(
        task_id="list_jobs", project_id=PROJECT_ID, results_filter="state=DONE"
    )
    get_job = CloudDLPGetDLPJobOperator(
        task_id="get_job",
        project_id=PROJECT_ID,
        dlp_job_id="{{ task_instance.xcom_pull('create_job')['name'].split('/')[-1] }}",
    )
    cancel_job = CloudDLPCancelDLPJobOperator(
        task_id="cancel_job",
        project_id=PROJECT_ID,
        dlp_job_id="{{ task_instance.xcom_pull('create_job')['name'].split('/')[-1] }}",
    )
    delete_job = CloudDLPDeleteDLPJobOperator(
        task_id="delete_job",
        project_id=PROJECT_ID,
        dlp_job_id="{{ task_instance.xcom_pull('create_job')['name'].split('/')[-1] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_job >> list_jobs >> get_job >> cancel_job >> delete_job
