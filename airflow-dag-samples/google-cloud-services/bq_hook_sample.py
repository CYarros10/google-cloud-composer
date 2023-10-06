"""
Airflow BQ Hook examples
"""

from airflow import models
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import  BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_15"
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
    "project_id": "your-project",
    "dataset": "reddit_stream_2",
    "source_table": "comments_stream_2",
}

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"bq_hook_example_{VERSION}",
    description=f"get bq job via a hook.",
    schedule_interval="0 0 * * *", # daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    

    def bq_row_count_validation(**context):

        bq_job_id = context['templates_dict']['bq_job_id']
        logging.info(bq_job_id)

        bq_job = BigQueryHook().get_job(
            project_id="your-project",
            location="US",
            job_id=bq_job_id
        )

        query = '''
                    INSERT INTO {dataset}.row_count_checks
                    (
                    SELECT 
                        "{dag_id}",
                        "{run_id}",
                        CAST("{logical_date}" as TIMESTAMP),
                        "{task_id}",
                        "{target_table}",
                        "{check_type}",
                        {num_affected_rows} as num_affected_rows,
                        "{bq_job_id}"
                    FROM {dataset}.{target_table}
                    )
                '''.format(
                        dataset=context['templates_dict']['dataset'],
                        dag_id=context['templates_dict']['dag_id'],
                        run_id=context['templates_dict']['run_id'],
                        logical_date=context['templates_dict']['logical_date'],
                        task_id=context['templates_dict']['task_id'],
                        target_table=context['templates_dict']['target_table'],
                        check_type="api",
                        num_affected_rows=int(bq_job.num_dml_affected_rows),
                        bq_job_id=bq_job_id
                    )

        logging.info(query)

        BigQueryHook().insert_job(
            configuration={
                "query": {
                    "query": query,
                    "useLegacySql": False,
                },
                "labels": {
                    "costcenter": "demos",
                    "application": "reddit_daily_agg",
                    "owner": "owner"
                }
            },
            project_id="your-project",
        )


    author_daily_aggregation_query_job = BigQueryInsertJobOperator(
        task_id="author_daily_aggregation_query_job",
        job_id="author_daily_aggregation_query_job",
        configuration={
            "query": {
                "query": "sql/author_daily_aggregation.sql",
                "useLegacySql": False,
            },
            "labels": {
                "costcenter": "demos",
                "application": "reddit_daily_agg",
                "owner": "owner"
            }
        },
        location="US",
        deferrable=True,
    )
    

    bq_row_count_validation_task = PythonOperator(
			task_id="bq_row_count_validation",
			python_callable=bq_row_count_validation,
            templates_dict={
                'bq_job_id': "{{task_instance.xcom_pull(task_ids='author_daily_aggregation_query_job', key='job_id')}}",
                'dataset': "{{ dataset }}",
                'dag_id': "{{dag.dag_id}}",
                'run_id': "{{run_id}}",
                'logical_date': "{{ts}}",
                'task_id': "{{ti.task_id}}",
                'target_table': "comments_partitioned",
            }
	)

    author_daily_aggregation_query_job >> bq_row_count_validation_task