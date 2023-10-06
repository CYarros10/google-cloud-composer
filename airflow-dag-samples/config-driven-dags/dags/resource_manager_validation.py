"""
Airflow Custom Operators for Resource Manager
"""

import logging
from airflow import models
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from utils.tooling import load_config
import os

#---------------------
# Config 
#---------------------
VERSION = "v1_0_0"

DAG_PATH = '/home/airflow/gcs/dags/'
config = load_config(
    path=os.path.join(DAG_PATH, 'configs/resource_manager_sample_config.yaml')
)

PROJECT = config['project']
ORG_ID= config['organization_id']
FOLDER_ID=config['folder_id']
VALID_FOLDER_REGEX=config['folder_regex_match']
VALID_PROJECT_REGEX=config['project_regex_match']

#-------------------------
# Tags, Default Args, and Macros
#-------------------------
tags = [
    f"project:{PROJECT}",
]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 9, 28),
    "sla": timedelta(minutes=25),
    #"gcp_conn_id": GCP_CONN_ID
}

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"rm_samples_{VERSION}",
    description=f"resource manager samples",
    schedule_interval="0 0 * * *", # daily midnight
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    from google.cloud import resourcemanager_v3
    import re
    def validate_project_ids(**context):
        folder_id = context['templates_dict']['folder_id']
        regex_validation_str = context['templates_dict']['valid_project_regex']

        valid_regex = re.compile(regex_validation_str)

        # List all projects you have access to
        client = resourcemanager_v3.ProjectsClient()
        page_result = client.list_projects(parent=f'folders/{folder_id}')

        # Handle the response
        for response in page_result:
            if re.match(valid_regex, response.display_name):
                logging.info(response)
            else:
                logging.info(f"Invalid Project ID: {response.display_name}")

    def validate_folder_ids(**context):

        org_id = context['templates_dict']['org_id']
        regex_validation_str = context['templates_dict']['valid_folder_regex']
    
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
            'folder_id': FOLDER_ID,
            'valid_project_regex': VALID_PROJECT_REGEX
        }
	)

    validate_folder_ids_task = PythonOperator(
        task_id="validate_folder_ids_task",
        python_callable=validate_folder_ids,
        templates_dict={
            'org_id': ORG_ID,
            'valid_folder_regex': VALID_FOLDER_REGEX
        }
	)

    validate_project_ids_task >> validate_folder_ids_task