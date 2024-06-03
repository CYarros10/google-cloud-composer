"""
Example Airflow DAG that creates, updates, queries and deletes a Cloud Spanner instance.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.spanner import (
    SpannerDeleteDatabaseInstanceOperator,
    SpannerDeleteInstanceOperator,
    SpannerDeployDatabaseInstanceOperator,
    SpannerDeployInstanceOperator,
    SpannerQueryDatabaseInstanceOperator,
    SpannerUpdateDatabaseInstanceOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_spanner"
GCP_SPANNER_INSTANCE_ID = f"instance-{DAG_ID}-{ENV_ID}".replace("_", "-")
GCP_SPANNER_DATABASE_ID = f"database-{DAG_ID}-{ENV_ID}".replace("_", "-")
GCP_SPANNER_CONFIG_NAME = f"projects/{PROJECT_ID}/instanceConfigs/regional-us-central1"
GCP_SPANNER_NODE_COUNT = 1
GCP_SPANNER_DISPLAY_NAME = "InstanceSpanner"
OPERATION_ID = "unique_operation_id"
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
    description="This Airflow DAG creates, updates, queries, and deletes a Cloud Spanner instance.",
    tags=["demo", "google_cloud", "spanner"],
) as dag:
    spanner_instance_create_task = SpannerDeployInstanceOperator(
        project_id=PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        configuration_name=GCP_SPANNER_CONFIG_NAME,
        node_count=GCP_SPANNER_NODE_COUNT,
        display_name=GCP_SPANNER_DISPLAY_NAME,
        task_id="spanner_instance_create_task",
    )
    spanner_instance_update_task = SpannerDeployInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        configuration_name=GCP_SPANNER_CONFIG_NAME,
        node_count=GCP_SPANNER_NODE_COUNT + 1,
        display_name=GCP_SPANNER_DISPLAY_NAME + "_updated",
        task_id="spanner_instance_update_task",
    )
    spanner_database_deploy_task = SpannerDeployDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        ddl_statements=[
            "CREATE TABLE my_table1 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
            "CREATE TABLE my_table2 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id="spanner_database_deploy_task",
    )
    spanner_database_update_task = SpannerUpdateDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        ddl_statements=[
            "CREATE TABLE my_table3 (id INT64, name STRING(MAX)) PRIMARY KEY (id)"
        ],
        task_id="spanner_database_update_task",
    )
    spanner_database_update_idempotent1_task = SpannerUpdateDatabaseInstanceOperator(
        project_id=PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        operation_id=OPERATION_ID,
        ddl_statements=[
            "CREATE TABLE my_table_unique (id INT64, name STRING(MAX)) PRIMARY KEY (id)"
        ],
        task_id="spanner_database_update_idempotent1_task",
    )
    spanner_database_update_idempotent2_task = SpannerUpdateDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        operation_id=OPERATION_ID,
        ddl_statements=[
            "CREATE TABLE my_table_unique (id INT64, name STRING(MAX)) PRIMARY KEY (id)"
        ],
        task_id="spanner_database_update_idempotent2_task",
    )
    spanner_instance_query_task = SpannerQueryDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        query=["DELETE FROM my_table2 WHERE true"],
        task_id="spanner_instance_query_task",
    )
    spanner_database_delete_task = SpannerDeleteDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        task_id="spanner_database_delete_task",
    )
    spanner_database_delete_task.trigger_rule = TriggerRule.ALL_DONE
    spanner_instance_delete_task = SpannerDeleteInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID, task_id="spanner_instance_delete_task"
    )
    spanner_instance_delete_task.trigger_rule = TriggerRule.ALL_DONE
    (
        spanner_instance_create_task
        >> spanner_instance_update_task
        >> spanner_database_deploy_task
        >> spanner_database_update_task
        >> spanner_database_update_idempotent1_task
        >> spanner_database_update_idempotent2_task
        >> spanner_instance_query_task
        >> spanner_database_delete_task
        >> spanner_instance_delete_task
    )
