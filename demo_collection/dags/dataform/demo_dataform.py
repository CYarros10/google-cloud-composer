"""
Example Airflow DAG for Google Cloud Dataform service
"""

from datetime import datetime, timedelta
from google.cloud.dataform_v1beta1 import WorkflowInvocation
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateRepositoryOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformCreateWorkspaceOperator,
    DataformDeleteRepositoryOperator,
    DataformDeleteWorkspaceOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
    DataformInstallNpmPackagesOperator,
    DataformMakeDirectoryOperator,
    DataformQueryWorkflowInvocationActionsOperator,
    DataformRemoveDirectoryOperator,
    DataformRemoveFileOperator,
    DataformWriteFileOperator,
)
from airflow.providers.google.cloud.sensors.dataform import (
    DataformWorkflowInvocationStateSensor,
)
from airflow.providers.google.cloud.utils.dataform import (
    make_initialization_workspace_flow,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataform"
REPOSITORY_ID = f"example_dataform_repository_{ENV_ID}"
LOCATION_REGION = "us-central1"
WORKSPACE_ID = f"example_dataform_workspace_{ENV_ID}"
DATAFORM_SCHEMA_NAME = f"schema_{DAG_ID}_{ENV_ID}"
with DAG(
    dag_id=DAG_ID,
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
    description="This Airflow DAG demonstrates how to use Google Cloud Dataform service to manage and transform data in Google BigQuery.",
    tags=["demo", "google_cloud", "dataform"],
) as dag:
    make_repository = DataformCreateRepositoryOperator(
        task_id="make-repository",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
    )
    make_workspace = DataformCreateWorkspaceOperator(
        task_id="make-workspace",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )
    first_initialization_step, last_initialization_step = (
        make_initialization_workspace_flow(
            project_id=PROJECT_ID,
            region=LOCATION_REGION,
            repository_id=REPOSITORY_ID,
            workspace_id=WORKSPACE_ID,
            package_name=f"dataform_package_{ENV_ID}",
            without_installation=True,
            dataform_schema_name=DATAFORM_SCHEMA_NAME,
        )
    )
    install_npm_packages = DataformInstallNpmPackagesOperator(
        task_id="install-npm-packages",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create-compilation-result",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}",
        },
    )
    get_compilation_result = DataformGetCompilationResultOperator(
        task_id="get-compilation-result",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        compilation_result_id="{{ task_instance.xcom_pull('create-compilation-result')['name'].split('/')[-1] }}",
    )
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )
    create_workflow_invocation_async = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation-async",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )
    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is-workflow-invocation-done",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull('create-workflow-invocation')['name'].split('/')[-1] }}",
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
    )
    get_workflow_invocation = DataformGetWorkflowInvocationOperator(
        task_id="get-workflow-invocation",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull('create-workflow-invocation')['name'].split('/')[-1] }}",
    )
    query_workflow_invocation_actions = DataformQueryWorkflowInvocationActionsOperator(
        task_id="query-workflow-invocation-actions",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull('create-workflow-invocation')['name'].split('/')[-1] }}",
    )
    create_workflow_invocation_for_cancel = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation-for-cancel",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
        asynchronous=True,
    )
    cancel_workflow_invocation = DataformCancelWorkflowInvocationOperator(
        task_id="cancel-workflow-invocation",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull('create-workflow-invocation-for-cancel')['name'].split('/')[-1] }}",
    )
    make_test_directory = DataformMakeDirectoryOperator(
        task_id="make-test-directory",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="test",
    )
    test_file_content = b"\n    test test for test file\n    "
    write_test_file = DataformWriteFileOperator(
        task_id="make-test-file",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        filepath="test/test.txt",
        contents=test_file_content,
    )
    remove_test_file = DataformRemoveFileOperator(
        task_id="remove-test-file",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        filepath="test/test.txt",
    )
    remove_test_directory = DataformRemoveDirectoryOperator(
        task_id="remove-test-directory",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="test",
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATAFORM_SCHEMA_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_workspace = DataformDeleteWorkspaceOperator(
        task_id="delete-workspace",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )
    delete_workspace.trigger_rule = TriggerRule.ALL_DONE
    delete_repository = DataformDeleteRepositoryOperator(
        task_id="delete-repository",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        repository_id=REPOSITORY_ID,
    )
    end = EmptyOperator(task_id="end")
    delete_repository.trigger_rule = TriggerRule.ALL_DONE
    make_repository >> make_workspace >> first_initialization_step
    (
        last_initialization_step
        >> install_npm_packages
        >> create_compilation_result
        >> get_compilation_result
        >> create_workflow_invocation
        >> get_workflow_invocation
        >> query_workflow_invocation_actions
        >> create_workflow_invocation_async
        >> is_workflow_invocation_done
        >> create_workflow_invocation_for_cancel
        >> cancel_workflow_invocation
        >> make_test_directory
        >> write_test_file
        >> remove_test_file
        >> remove_test_directory
        >> delete_dataset
        >> delete_workspace
        >> delete_repository
    )
    remove_test_directory >> end
