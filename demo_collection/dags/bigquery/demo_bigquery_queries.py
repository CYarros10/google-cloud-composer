"""
Example Airflow DAG for Google BigQuery service.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryColumnCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryTableCheckOperator,
    BigQueryValueCheckOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"
QUERY_SQL_PATH = "resources/demo_bq_example_bigquery_query.sql"
TABLE_1 = "table1"
TABLE_2 = "table2"
SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]
DAGS_LIST = []
locations = [None, LOCATION_REGION]
for index, location in enumerate(locations, 1):
    DAG_ID = "demo_bigquery_queries_location" if location else "demo_bigquery_queries"
    DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
    DATASET = f"{DATASET_NAME}{index}"
    INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
    INSERT_ROWS_QUERY = f"INSERT {DATASET}.{TABLE_1} VALUES (42, 'monty python', '{INSERT_DATE}'), (42, 'fishy fish', '{INSERT_DATE}');"
    with DAG(
        DAG_ID,
        schedule="@once",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        is_paused_upon_creation=True,
        user_defined_macros={
            "DATASET": DATASET,
            "TABLE": TABLE_1,
            "QUERY_SQL_PATH": QUERY_SQL_PATH,
        },
        dagrun_timeout=timedelta(minutes=60),
        max_active_runs=1,
        default_args={
            "owner": "Google",
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=1),
            "sla": timedelta(minutes=55),
        },
        description="This DAG demonstrates Google BigQuery interactions through dataset & table creation, data insertion & retrieval, and data validation.  Is there anything specific about this DAG you would like to know more about? \n",
        tags=["demo", "google_cloud", "bigquery"],
    ) as dag:
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_dataset", dataset_id=DATASET, location=location
        )
        create_table_1 = BigQueryCreateEmptyTableOperator(
            task_id="create_table_1",
            dataset_id=DATASET,
            table_id=TABLE_1,
            schema_fields=SCHEMA,
            location=location,
        )
        create_table_2 = BigQueryCreateEmptyTableOperator(
            task_id="create_table_2",
            dataset_id=DATASET,
            table_id=TABLE_2,
            schema_fields=SCHEMA,
            location=location,
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
            location=location,
        )
        select_query_job = BigQueryInsertJobOperator(
            task_id="select_query_job",
            configuration={
                "query": {
                    "query": "{% include QUERY_SQL_PATH %}",
                    "useLegacySql": False,
                }
            },
            location=location,
        )
        execute_insert_query = BigQueryInsertJobOperator(
            task_id="execute_insert_query",
            configuration={
                "query": {"query": INSERT_ROWS_QUERY, "useLegacySql": False}
            },
            location=location,
        )
        execute_query_save = BigQueryInsertJobOperator(
            task_id="execute_query_save",
            configuration={
                "query": {
                    "query": f"SELECT * FROM {DATASET}.{TABLE_1}",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET,
                        "tableId": TABLE_2,
                    },
                }
            },
            location=location,
        )
        bigquery_execute_multi_query = BigQueryInsertJobOperator(
            task_id="execute_multi_query",
            configuration={
                "query": {
                    "query": [
                        f"SELECT * FROM {DATASET}.{TABLE_2}",
                        f"SELECT COUNT(*) FROM {DATASET}.{TABLE_2}",
                    ],
                    "useLegacySql": False,
                }
            },
            location=location,
        )
        get_data = BigQueryGetDataOperator(
            task_id="get_data",
            dataset_id=DATASET,
            table_id=TABLE_1,
            max_results=10,
            selected_fields="value,name",
            location=location,
        )
        get_data_result = BashOperator(
            task_id="get_data_result", bash_command=f"echo {get_data.output}"
        )
        check_count = BigQueryCheckOperator(
            task_id="check_count",
            sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE_1}",
            use_legacy_sql=False,
            location=location,
        )
        check_value = BigQueryValueCheckOperator(
            task_id="check_value",
            sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE_1}",
            pass_value=4,
            use_legacy_sql=False,
            location=location,
        )
        check_interval = BigQueryIntervalCheckOperator(
            task_id="check_interval",
            table=f"{DATASET}.{TABLE_1}",
            days_back=1,
            metrics_thresholds={"COUNT(*)": 1.5},
            use_legacy_sql=False,
            location=location,
        )
        column_check = BigQueryColumnCheckOperator(
            task_id="column_check",
            table=f"{DATASET}.{TABLE_1}",
            column_mapping={"value": {"null_check": {"equal_to": 0}}},
        )
        table_check = BigQueryTableCheckOperator(
            task_id="table_check",
            table=f"{DATASET}.{TABLE_1}",
            checks={"row_count_check": {"check_statement": "COUNT(*) = 4"}},
        )
        delete_dataset = BigQueryDeleteDatasetOperator(
            task_id="delete_dataset",
            dataset_id=DATASET,
            delete_contents=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        end = EmptyOperator(task_id="end")
        create_dataset >> [create_table_1, create_table_2]
        (
            [create_table_1, create_table_2]
            >> insert_query_job
            >> [select_query_job, execute_insert_query]
        )
        execute_insert_query >> get_data >> get_data_result >> delete_dataset
        get_data_result >> end
        (
            execute_insert_query
            >> execute_query_save
            >> bigquery_execute_multi_query
            >> [delete_dataset, end]
        )
        (
            execute_insert_query
            >> [check_count, check_value, check_interval]
            >> delete_dataset
        )
        [check_count, check_value, check_interval] >> end
        execute_insert_query >> [column_check, table_check] >> delete_dataset
        [column_check, table_check] >> end
    DAGS_LIST.append(dag)
    globals()[DAG_ID] = dag
