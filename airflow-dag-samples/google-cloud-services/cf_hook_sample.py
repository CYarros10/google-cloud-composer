"""
Airflow CF Hook examples
"""

import json
import logging
from airflow import models
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.functions import  CloudFunctionsHook

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_8"
PROJECT = "your-project"
TEAM="google"
BUNDLE="demo"
COMPOSER_ID="demo"
ORG="demo"

#-------------------------
# Tags, Default Args, and Macros
#-------------------------
tags = [
    f"bundle:{BUNDLE}",
    f"project:{PROJECT}",
    f"team:{TEAM}",
    f"org:{ORG}",
    f"composer_id:{COMPOSER_ID}"
]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 2, 25),
    "sla": timedelta(minutes=25),
    #"gcp_conn_id": GCP_CONN_ID
}

user_defined_macros = {
    "project_id": f"{PROJECT}",
}

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"cf_hook_example_{VERSION}",
    description=f"invoke cloud function via a hook.",
    schedule_interval="0 0 * * *", # daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    

    def invoke_cloud_function(**context):
        output_lof = context['templates_dict']['output_lof']
        dest_bucket_name = context['templates_dict']['dest_bucket_name']

        logging.info(output_lof)
        logging.info(dest_bucket_name)

        hook = CloudFunctionsHook(
            api_version="v1"
        )

        function = hook.get_function(
            name="projects/your-project/locations/us-central1/functions/bz2-decompress"
        )

        logging.info(function)
        

        payload = {"lof_path": output_lof, "dest_bucket_name": dest_bucket_name}

        logging.info(json.dumps(payload))

        hook.call_function(
            function_id="bz2-decompress",
            input_data=payload,
            location="us-central1",
            project_id="your-project"

        )

    invoke_cloud_function_task = PythonOperator(
			task_id="invoke_cloud_function_task",
			python_callable=invoke_cloud_function,
            templates_dict={
                'output_lof': "gs://your-bucket/cloud_function/output_lof.csv",
                'dest_bucket_name': "gs://your-bucket/decompress"
            }
	)

    invoke_cloud_function_task