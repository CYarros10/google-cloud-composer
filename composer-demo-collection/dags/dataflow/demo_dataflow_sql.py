"""
Example Airflow DAG for Google Cloud Dataflow service
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartSqlJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "cy-artifacts"
ENV_ID = "composer"
DAG_ID = "demo_dataflow_sql"
LOCATION_REGION = "us-central1"
DATAFLOW_SQL_JOB_NAME = f"{DAG_ID}_{ENV_ID}".replace("_", "-")
BQ_SQL_DATASET = f"{DAG_ID}_{ENV_ID}".replace("-", "_")
BQ_SQL_TABLE_INPUT = f"input_{ENV_ID}".replace("-", "_")
BQ_SQL_TABLE_OUTPUT = f"output_{ENV_ID}".replace("-", "_")
INSERT_ROWS_QUERY = f"INSERT {BQ_SQL_DATASET}.{BQ_SQL_TABLE_INPUT} VALUES ('John Doe', 900), ('Alice Storm', 1200),('Bob Max', 1000),('Peter Jackson', 800),('Mia Smith', 1100);"
with DAG(
    dag_id=DAG_ID,
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
    description="start sql job that insert into bigquery table, insert query and delete query also executed",
    tags=["demo", "google_cloud", "dataflow", "sql"],
) as dag:
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset", dataset_id=BQ_SQL_DATASET, location=LOCATION_REGION
    )
    create_bq_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        dataset_id=BQ_SQL_DATASET,
        table_id=BQ_SQL_TABLE_INPUT,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
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
    )
    start_sql = DataflowStartSqlJobOperator(
        task_id="start_sql_query",
        job_name=DATAFLOW_SQL_JOB_NAME,
        query=f"\n            SELECT\n                emp_name as employee,\n                salary as employee_salary\n            FROM\n                bigquery.table.`{PROJECT_ID}`.`{BQ_SQL_DATASET}`.`{BQ_SQL_TABLE_INPUT}`\n            WHERE salary >= 1000;\n        ",
        options={
            "bigquery-project": PROJECT_ID,
            "bigquery-dataset": BQ_SQL_DATASET,
            "bigquery-table": BQ_SQL_TABLE_OUTPUT,
        },
        location=LOCATION_REGION,
        do_xcom_push=True,
    )
    delete_bq_table = BigQueryDeleteTableOperator(
        task_id="delete_bq_table",
        deletion_dataset_table=f"{PROJECT_ID}.{BQ_SQL_DATASET}.{BQ_SQL_TABLE_INPUT}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bq_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_bq_dataset",
        dataset_id=BQ_SQL_DATASET,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_bq_dataset
        >> create_bq_table
        >> insert_query_job
        >> start_sql
        >> delete_bq_table
        >> delete_bq_dataset
    )
    start_sql >> end
