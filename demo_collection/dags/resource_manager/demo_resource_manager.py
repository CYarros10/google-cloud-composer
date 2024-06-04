"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

import logging
from airflow import models
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# ---------------------
# Config
# ---------------------
PROJECT = "<your project>"
TEAM = "google"
ORG_ID = "<your org id number>"
FOLDER_ID = "<your folder id number"
VALID_FOLDER_REGEX = "<your regex match>"
VALID_PROJECT_REGEX = "<your regex match>"

with models.DAG(
    "demo_resource_manager",
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
    description="This Airflow DAG validates project ids and folders within resource manager",
    tags=["demo", "google_cloud", "resource_manager", "python"],
) as dag:
    from google.cloud import resourcemanager_v3
    import re

    def validate_project_ids(**context):
        folder_id = context["templates_dict"]["folder_id"]
        regex_validation_str = context["templates_dict"]["valid_project_regex"]

        valid_regex = re.compile(regex_validation_str)

        # List all projects you have access to
        client = resourcemanager_v3.ProjectsClient()
        page_result = client.list_projects(parent=f"folders/{folder_id}")

        # Handle the response
        for response in page_result:
            if re.match(valid_regex, response.display_name):
                logging.info(response)
            else:
                logging.info(f"Invalid Project ID: {response.display_name}")

    def validate_folder_ids(**context):
        org_id = context["templates_dict"]["org_id"]
        regex_validation_str = context["templates_dict"]["valid_folder_regex"]

        # Create a client
        client = resourcemanager_v3.FoldersClient()

        # Initialize request argument(s)
        request = resourcemanager_v3.ListFoldersRequest(
            parent=f"organizations/{org_id}",
        )

        # Make the request
        page_result = client.list_folders(request=request)

        # Handle the response
        for response in page_result:
            if re.match(regex_validation_str, response.display_name):
                logging.info(response)
            else:
                logging.info(f"Invalid Folder ID: {response.display_name}")

    validate_project_ids_task = PythonOperator(
        task_id="validate_project_ids_task",
        python_callable=validate_project_ids,
        templates_dict={
            "folder_id": FOLDER_ID,
            "valid_project_regex": VALID_PROJECT_REGEX,
        },
    )

    validate_folder_ids_task = PythonOperator(
        task_id="validate_folder_ids_task",
        python_callable=validate_folder_ids,
        templates_dict={"org_id": ORG_ID, "valid_folder_regex": VALID_FOLDER_REGEX},
    )

    validate_project_ids_task >> validate_folder_ids_task
