"""
Apache Airflow DAG for example BigQuery Optimization
"""

from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

#---------------------
# Universal DAG info
#---------------------
VERSION = "v1_0_0"
PROJECT = "cy-artifacts"
IMPERSONATION_CHAIN="bigquery-optimizer-186@cy-artifacts.iam.gserviceaccount.com"

TEAM="pso"
BUNDLE="demo"
COMPOSER_ID="demo"
ORG="googlecloud"

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
    "start_date": datetime(2023,8,1),
    "sla": timedelta(minutes=30),
    "deferrable":False,
}
user_defined_macros = {
    "project_id": PROJECT,
    "bq_service_account": IMPERSONATION_CHAIN
}

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"bq_optimization_workshop_{VERSION}",
    description="generate bigquery optimization information",
    schedule_interval="0 0 * * *", # daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30)
) as dag:

    info_schema_job = BigQueryInsertJobOperator(
        task_id="info_schema_job",
        job_id="info_schema_job",
        configuration={
            "query": {
                "query": "select * FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT limit 10",
                "useLegacySql": False,
            },
            "labels": {
                "costcenter": "12345",
                "application": "demos",
                "owner": "googlepso"
            }
        },
        location="US",
        impersonation_chain="{{ bq_service_account }}",
    )

    info_schema_job
