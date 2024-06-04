"""
Example Airflow DAG for Google BigQuery service.
Uses Async version of the Big Query Operators
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_bigquery_queries_async"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")
LOCATION_REGION = "us"
TABLE_NAME_1 = f"table_{DAG_ID}_{ENV_ID}_1".replace("-", "_")
TABLE_NAME_2 = f"table_{DAG_ID}_{ENV_ID}_2".replace("-", "_")
SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "STRING", "mode": "NULLABLE"},
]
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = f"INSERT {DATASET_NAME}.{TABLE_NAME_1} VALUES (42, 'monthy python', '{INSERT_DATE}'), (42, 'fishy fish', '{INSERT_DATE}');"
CONFIGURATION = {
    "query": {
        "query": f"DECLARE success BOOL;\n        DECLARE size_bytes INT64;\n        DECLARE row_count INT64;\n        DECLARE DELAY_TIME DATETIME;\n        DECLARE WAIT STRING;\n        SET success = FALSE;\n\n        SELECT row_count = (SELECT row_count FROM {DATASET_NAME}.__TABLES__\n        WHERE table_id='NON_EXISTING_TABLE');\n        IF row_count > 0  THEN\n            SELECT 'Table Exists!' as message, retry_count as retries;\n            SET success = TRUE;\n        ELSE\n            SELECT 'Table does not exist' as message, row_count;\n            SET WAIT = 'TRUE';\n            SET DELAY_TIME = DATETIME_ADD(CURRENT_DATETIME,INTERVAL 1 MINUTE);\n            WHILE WAIT = 'TRUE' DO\n                IF (DELAY_TIME < CURRENT_DATETIME) THEN\n                    SET WAIT = 'FALSE';\n                END IF;\n            END WHILE;\n        END IF;",
        "useLegacySql": False,
    }
}
with DAG(
    dag_id=DAG_ID,
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
    user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_NAME_1},
    description="This DAG demonstrates async BigQuery operators for inserting, querying, checking, and managing data. ",
    tags=["demo", "google_cloud", "bigquery", "async"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, location=LOCATION_REGION
    )
    create_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME_1,
        schema_fields=SCHEMA,
        location=LOCATION_REGION,
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION_REGION,
        deferrable=True,
    )
    select_query_job = BigQueryInsertJobOperator(
        task_id="select_query_job",
        configuration={
            "query": {
                "query": "{% include 'resources/demo_bq_example_bigquery_query.sql' %}",
                "useLegacySql": False,
            }
        },
        location=LOCATION_REGION,
        deferrable=True,
    )
    check_value = BigQueryValueCheckOperator(
        task_id="check_value",
        sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_NAME_1}",
        pass_value=2,
        use_legacy_sql=False,
        location=LOCATION_REGION,
        deferrable=True,
    )
    check_interval = BigQueryIntervalCheckOperator(
        task_id="check_interval",
        table=f"{DATASET_NAME}.{TABLE_NAME_1}",
        days_back=1,
        metrics_thresholds={"COUNT(*)": 1.5},
        use_legacy_sql=False,
        location=LOCATION_REGION,
        deferrable=True,
    )
    bigquery_execute_multi_query = BigQueryInsertJobOperator(
        task_id="execute_multi_query",
        configuration={
            "query": {
                "query": [
                    f"SELECT * FROM {DATASET_NAME}.{TABLE_NAME_2}",
                    f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_NAME_2}",
                ],
                "useLegacySql": False,
            }
        },
        location=LOCATION_REGION,
        deferrable=True,
    )
    get_data = BigQueryGetDataOperator(
        task_id="get_data",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME_1,
        use_legacy_sql=False,
        max_results=10,
        selected_fields="value",
        location=LOCATION_REGION,
        deferrable=True,
    )
    get_data_result = BashOperator(
        task_id="get_data_result",
        bash_command=f"echo {get_data.output}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    check_count = BigQueryCheckOperator(
        task_id="check_count",
        sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{TABLE_NAME_1}",
        use_legacy_sql=False,
        location=LOCATION_REGION,
        deferrable=True,
    )
    execute_query_save = BigQueryInsertJobOperator(
        task_id="execute_query_save",
        configuration={
            "query": {
                "query": f"SELECT * FROM {DATASET_NAME}.{TABLE_NAME_1}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_NAME,
                    "tableId": TABLE_NAME_2,
                },
            }
        },
        location=LOCATION_REGION,
        deferrable=True,
    )
    execute_long_running_query = BigQueryInsertJobOperator(
        task_id="execute_long_running_query",
        configuration=CONFIGURATION,
        location=LOCATION_REGION,
        deferrable=True,
    )
    end = EmptyOperator(task_id="end")
    create_dataset >> create_table_1 >> insert_query_job
    insert_query_job >> select_query_job >> check_count
    insert_query_job >> get_data >> get_data_result
    insert_query_job >> execute_query_save >> bigquery_execute_multi_query
    insert_query_job >> execute_long_running_query >> check_value >> check_interval
    [
        check_count,
        check_interval,
        bigquery_execute_multi_query,
        get_data_result,
    ] >> delete_dataset
    [check_count, check_interval, bigquery_execute_multi_query, get_data_result] >> end
