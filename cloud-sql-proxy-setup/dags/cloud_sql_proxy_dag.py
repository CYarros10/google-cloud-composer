"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""
import logging as log

from datetime import timedelta, datetime
from airflow import models

from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

# -------------------------
# Tags, Default Args, and Macros
# -------------------------
tags = ["application:samples"]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 11, 20),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"cloud_sql_proxy_dag_{VERSION}",
    description="Sample DAG for various Cloud SQL tasks via a SQL Proxy.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    def _my_sql_test():

        connection = MySqlHook(mysql_conn_id='cloud_sql_proxy_service').get_conn()
        cursor = connection.cursor()
        cursor.execute('select * from sample_table where id = 1')

        for row in cursor:
            print('Start referencedb_test connection')
            log.info('Row %s', row)
            print('End referencedb_test connection')

    get_data_task = PythonOperator(
        task_id='get_data_task',
        python_callable=_my_sql_test,
    )
