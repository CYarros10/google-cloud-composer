from datetime import datetime, timedelta
from typing import cast
from google.protobuf.field_mask_pb2 import FieldMask
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.workflows import (
    WorkflowsCancelExecutionOperator,
    WorkflowsCreateExecutionOperator,
    WorkflowsCreateWorkflowOperator,
    WorkflowsDeleteWorkflowOperator,
    WorkflowsGetExecutionOperator,
    WorkflowsGetWorkflowOperator,
    WorkflowsListExecutionsOperator,
    WorkflowsListWorkflowsOperator,
    WorkflowsUpdateWorkflowOperator,
)
from airflow.providers.google.cloud.sensors.workflows import WorkflowExecutionSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_cloud_workflows"
LOCATION_REGION = "us-central1"
WORKFLOW_ID = f"workflow-{DAG_ID}-{ENV_ID}".replace("_", "-")
WORKFLOW_CONTENT = '\n- getLanguage:\n    assign:\n        - inputLanguage: "English"\n- readWikipedia:\n    call: http.get\n    args:\n        url: https://www.wikipedia.org/\n        query:\n            action: opensearch\n            search: ${inputLanguage}\n    result: wikiResult\n- returnResult:\n    return: ${wikiResult}\n'
WORKFLOW = {
    "description": "Test workflow",
    "labels": {"airflow-version": "dev"},
    "source_contents": WORKFLOW_CONTENT,
}
EXECUTION = {"argument": ""}
SLEEP_WORKFLOW_ID = f"sleep-workflow-{DAG_ID}-{ENV_ID}".replace("_", "-")
SLEEP_WORKFLOW_CONTENT = (
    "\n- someSleep:\n    call: sys.sleep\n    args:\n        seconds: 120\n"
)
SLEEP_WORKFLOW = {
    "description": "Test workflow",
    "labels": {"airflow-version": "dev"},
    "source_contents": SLEEP_WORKFLOW_CONTENT,
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
    description="This DAG creates, updates, gets, lists, and deletes workflows and executions.",
    tags=["demo", "google_cloud", "workflows"],
) as dag:
    create_workflow = WorkflowsCreateWorkflowOperator(
        task_id="create_workflow",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow=WORKFLOW,
        workflow_id=WORKFLOW_ID,
    )
    update_workflow = WorkflowsUpdateWorkflowOperator(
        task_id="update_workflow",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        update_mask=FieldMask(paths=["name", "description"]),
    )
    get_workflow = WorkflowsGetWorkflowOperator(
        task_id="get_workflow",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
    )
    list_workflows = WorkflowsListWorkflowsOperator(
        task_id="list_workflows", location=LOCATION_REGION, project_id=PROJECT_ID
    )
    delete_workflow = WorkflowsDeleteWorkflowOperator(
        task_id="delete_workflow",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_execution = WorkflowsCreateExecutionOperator(
        task_id="create_execution",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        execution=EXECUTION,
        workflow_id=WORKFLOW_ID,
    )
    create_execution_id = cast(str, XComArg(create_execution, key="execution_id"))
    wait_for_execution = WorkflowExecutionSensor(
        task_id="wait_for_execution",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution_id,
    )
    get_execution = WorkflowsGetExecutionOperator(
        task_id="get_execution",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution_id,
    )
    list_executions = WorkflowsListExecutionsOperator(
        task_id="list_executions",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
    )
    create_workflow_for_cancel = WorkflowsCreateWorkflowOperator(
        task_id="create_workflow_for_cancel",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow=SLEEP_WORKFLOW,
        workflow_id=SLEEP_WORKFLOW_ID,
    )
    create_execution_for_cancel = WorkflowsCreateExecutionOperator(
        task_id="create_execution_for_cancel",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        execution=EXECUTION,
        workflow_id=SLEEP_WORKFLOW_ID,
    )
    cancel_execution_id = cast(
        str, XComArg(create_execution_for_cancel, key="execution_id")
    )
    cancel_execution = WorkflowsCancelExecutionOperator(
        task_id="cancel_execution",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=SLEEP_WORKFLOW_ID,
        execution_id=cancel_execution_id,
    )
    delete_workflow_for_cancel = WorkflowsDeleteWorkflowOperator(
        task_id="delete_workflow_for_cancel",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        workflow_id=SLEEP_WORKFLOW_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_workflow >> update_workflow >> [get_workflow, list_workflows]
    update_workflow >> [create_execution, create_execution_for_cancel]
    wait_for_execution >> [get_execution, list_executions]
    (
        create_workflow_for_cancel
        >> create_execution_for_cancel
        >> cancel_execution
        >> delete_workflow_for_cancel
    )
    [cancel_execution, list_executions] >> delete_workflow
