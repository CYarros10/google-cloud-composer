"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLExecuteQueryOperator,
    CloudSQLExportInstanceOperator
)
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.dbapi import DbApiHook
import logging as log

 

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_8"

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
    f"cloudsql_dag_{VERSION}",
    description="Sample DAG for various Cloud SQL tasks.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    # this is best used for DDL. won't return results
    cloud_sql_drop_table_task = CloudSQLExecuteQueryOperator(
        task_id="cloud_sql_drop_table_task",
        gcp_cloudsql_conn_id="airflow_to_sql_test",
        sql="""
            DROP TABLE sample_table;
        """,
    )

    # this is best used for DDL. won't return results
    cloud_sql_create_table_task = CloudSQLExecuteQueryOperator(
        task_id="cloud_sql_create_table_task",
        gcp_cloudsql_conn_id="airflow_to_sql_test",
        sql="""
            CREATE TABLE sample_table (
                name VARCHAR(50),           -- Text field with maximum length of 50 characters 
                email VARCHAR(100),         -- Email field
                age INT                     -- Age (integer)
            );
        """,
        trigger_rule='all_done'
    )

    # this is best used for DDL. won't return results
    cloud_sql_insert_task = CloudSQLExecuteQueryOperator(
        task_id="cloud_sql_insert_task",
        gcp_cloudsql_conn_id="airflow_to_sql_test",
        sql="""
            INSERT INTO sample_table VALUES ('cy', 'cy@g.com', 25);
        """,
        trigger_rule='all_done'
    )

    # The Cloud SQL instance needs Storage Object User/Creator role
    sql_export_task = CloudSQLExportInstanceOperator(
        task_id="sql_export_task",
        instance="airflow-connect", 
        body={
            "exportContext": {
                "fileType": "csv",
                "uri": f"gs://cy-sandbox/sql-export/from-airflow.csv",
                "csvExportOptions": {
                    "selectQuery": "SELECT * FROM `airflow_db`.`sample_table`;",
                },
            }
        }, 
    )

    cloud_sql_drop_table_task >> cloud_sql_create_table_task >> cloud_sql_insert_task >> sql_export_task