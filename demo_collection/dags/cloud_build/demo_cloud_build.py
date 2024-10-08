"""
Example Airflow DAG that displays interactions with Google Cloud Build.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast
import yaml
from airflow.decorators import task_group
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCancelBuildOperator,
    CloudBuildCreateBuildOperator,
    CloudBuildGetBuildOperator,
    CloudBuildListBuildsOperator,
    CloudBuildRetryBuildOperator,
)

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_cloud_build"
GCP_SOURCE_ARCHIVE_URL = "gs://airflow-system-tests-resources/cloud-build/file.tar.gz"
GCP_SOURCE_REPOSITORY_NAME = "test-cloud-build-repo"
CURRENT_FOLDER = Path(__file__).parent
CREATE_BUILD_FROM_STORAGE_BODY = {
    "source": {"storage_source": GCP_SOURCE_ARCHIVE_URL},
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world"]}],
}
CREATE_BUILD_FROM_REPO_BODY: dict[str, Any] = {
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
    description="The DAG demonstrates how to interact with Google Cloud Build using CloudBuildCreateBuildOperator, CloudBuildCancelBuildOperator, CloudBuildRetryBuildOperator, and CloudBuildGetBuildOperator.\n",
    tags=["demo", "google_cloud", "cloud_build"],
) as dag:

    @task_group(group_id="build_from_storage")
    def build_from_storage():
        create_build_from_storage = CloudBuildCreateBuildOperator(
            task_id="create_build_from_storage",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_STORAGE_BODY,
        )
        create_build_from_storage_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_storage, key='results'))}",
            task_id="create_build_from_storage_result",
        )
        create_build_from_storage >> create_build_from_storage_result

    @task_group(group_id="build_from_storage_deferrable")
    def build_from_storage_deferrable():
        create_build_from_storage = CloudBuildCreateBuildOperator(
            task_id="create_build_from_storage",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_STORAGE_BODY,
            deferrable=True,
        )
        create_build_from_storage_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_storage, key='results'))}",
            task_id="create_build_from_storage_result",
        )
        create_build_from_storage >> create_build_from_storage_result

    @task_group(group_id="build_from_repo")
    def build_from_repo():
        create_build_from_repo = CloudBuildCreateBuildOperator(
            task_id="create_build_from_repo",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
        )
        create_build_from_repo_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
            task_id="create_build_from_repo_result",
        )
        create_build_from_repo >> create_build_from_repo_result

    @task_group(group_id="build_from_repo_deferrable")
    def build_from_repo_deferrable():
        create_build_from_repo = CloudBuildCreateBuildOperator(
            task_id="create_build_from_repo",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
            deferrable=True,
        )
        create_build_from_repo_result = BashOperator(
            bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
            task_id="create_build_from_repo_result",
        )
        create_build_from_repo >> create_build_from_repo_result

    create_build_from_file = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file",
        project_id=PROJECT_ID,
        build=yaml.safe_load(
            (
                Path(CURRENT_FOLDER) / "resources" / "example_cloud_build.yaml"
            ).read_text()
        ),
        params={"name": "Airflow"},
    )
    create_build_from_file_deferrable = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file_deferrable",
        project_id=PROJECT_ID,
        build=yaml.safe_load(
            (
                Path(CURRENT_FOLDER) / "resources" / "example_cloud_build.yaml"
            ).read_text()
        ),
        params={"name": "Airflow"},
        deferrable=True,
    )
    list_builds = CloudBuildListBuildsOperator(
        task_id="list_builds", project_id=PROJECT_ID, location="global"
    )

    @task_group(group_id="no_wait_cancel_retry_get")
    def no_wait_cancel_retry_get():
        create_build_without_wait = CloudBuildCreateBuildOperator(
            task_id="create_build_without_wait",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
            wait=False,
        )
        cancel_build = CloudBuildCancelBuildOperator(
            task_id="cancel_build",
            id_=cast(str, XComArg(create_build_without_wait, key="id")),
            project_id=PROJECT_ID,
        )
        retry_build = CloudBuildRetryBuildOperator(
            task_id="retry_build",
            id_=cast(str, XComArg(cancel_build, key="id")),
            project_id=PROJECT_ID,
        )
        get_build = CloudBuildGetBuildOperator(
            task_id="get_build",
            id_=cast(str, XComArg(retry_build, key="id")),
            project_id=PROJECT_ID,
        )
        create_build_without_wait >> cancel_build >> retry_build >> get_build

    @task_group(group_id="no_wait_cancel_retry_get_deferrable")
    def no_wait_cancel_retry_get_deferrable():
        create_build_without_wait = CloudBuildCreateBuildOperator(
            task_id="create_build_without_wait",
            project_id=PROJECT_ID,
            build=CREATE_BUILD_FROM_REPO_BODY,
            wait=False,
            deferrable=True,
        )
        cancel_build = CloudBuildCancelBuildOperator(
            task_id="cancel_build",
            id_=cast(str, XComArg(create_build_without_wait, key="id")),
            project_id=PROJECT_ID,
        )
        retry_build = CloudBuildRetryBuildOperator(
            task_id="retry_build",
            id_=cast(str, XComArg(cancel_build, key="id")),
            project_id=PROJECT_ID,
        )
        get_build = CloudBuildGetBuildOperator(
            task_id="get_build",
            id_=cast(str, XComArg(retry_build, key="id")),
            project_id=PROJECT_ID,
        )
        create_build_without_wait >> cancel_build >> retry_build >> get_build

    (
        [
            build_from_storage(),
            build_from_storage_deferrable(),
            build_from_repo(),
            build_from_repo_deferrable(),
            create_build_from_file,
            create_build_from_file_deferrable,
        ]
        >> list_builds
        >> [no_wait_cancel_retry_get(), no_wait_cancel_retry_get_deferrable()]
    )
