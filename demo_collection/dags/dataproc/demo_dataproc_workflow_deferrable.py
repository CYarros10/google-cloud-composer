"""
Example Airflow DAG for Dataproc workflow operators.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateWorkflowTemplateOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
)

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_dataproc_wf_def"
LOCATION_REGION = "us-central1"
CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}".replace("_", "")
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}
PIG_JOB = {"query_list": {"queries": ["define sin HiveUDF('sin');"]}}
WORKFLOW_NAME = "airflow-dataproc-test-def"
WORKFLOW_TEMPLATE = {
    "id": WORKFLOW_NAME,
    "placement": {
        "managed_cluster": {"cluster_name": CLUSTER_NAME, "config": CLUSTER_CONFIG}
    },
    "jobs": [{"step_id": "pig_job_1", "pig_job": PIG_JOB}],
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
    description=" This Airflow DAG creates and runs a Dataproc workflow with a Pig job.  (9 words)",
    tags=["demo", "google_cloud", "dataproc", "pig"],
) as dag:
    create_workflow_template = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template",
        template=WORKFLOW_TEMPLATE,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
    )
    trigger_workflow_async = DataprocInstantiateWorkflowTemplateOperator(
        task_id="trigger_workflow_async",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        template_id=WORKFLOW_NAME,
        deferrable=True,
    )
    instantiate_inline_workflow_template_async = (
        DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id="instantiate_inline_workflow_template_async",
            template=WORKFLOW_TEMPLATE,
            region=LOCATION_REGION,
            deferrable=True,
        )
    )
    (
        create_workflow_template
        >> trigger_workflow_async
        >> instantiate_inline_workflow_template_async
    )
