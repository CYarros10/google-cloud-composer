"""
Example Airflow DAG for Google BigQuery Sensors.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceAsyncSensor,
    BigQueryTableExistencePartitionAsyncSensor,
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_bigquery_sensors"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")
TABLE_NAME = f"partitioned_table_{DAG_ID}_{ENV_ID}".replace("-", "_")
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
PARTITION_NAME = "{{ ds_nodash }}"
INSERT_ROWS_QUERY = f"INSERT {DATASET_NAME}.{TABLE_NAME} VALUES (42, '{{{{ ds }}}}')"
SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=25),
        "project_id": PROJECT_ID,
    },
    user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_NAME},
    description="Table and partition existence checks in BigQuery.",
    tags=["google_cloud", "bigquery", "sensors"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=SCHEMA,
        time_partitioning={"type": "DAY", "field": "ds"},
    )
    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_table_exists",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    check_table_exists_def = BigQueryTableExistenceSensor(
        task_id="check_table_exists_def",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        deferrable=True,
    )
    check_table_exists_async = BigQueryTableExistenceAsyncSensor(
        task_id="check_table_exists_async",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    execute_insert_query = BigQueryInsertJobOperator(
        task_id="execute_insert_query",
        configuration={"query": {"query": INSERT_ROWS_QUERY, "useLegacySql": False}},
    )
    check_table_partition_exists = BigQueryTablePartitionExistenceSensor(
        task_id="check_table_partition_exists",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        partition_id=PARTITION_NAME,
    )
    check_table_partition_exists_def = BigQueryTablePartitionExistenceSensor(
        task_id="check_table_partition_exists_def",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        partition_id=PARTITION_NAME,
        deferrable=True,
    )
    check_table_partition_exists_async = BigQueryTableExistencePartitionAsyncSensor(
        task_id="check_table_partition_exists_async",
        partition_id=PARTITION_NAME,
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_dataset
        >> create_table
        >> [check_table_exists, check_table_exists_async, check_table_exists_def]
        >> execute_insert_query
        >> [
            check_table_partition_exists,
            check_table_partition_exists_async,
            check_table_partition_exists_def,
        ]
        >> delete_dataset
    )
    [
        check_table_partition_exists,
        check_table_partition_exists_async,
        check_table_partition_exists_def,
    ] >> end
