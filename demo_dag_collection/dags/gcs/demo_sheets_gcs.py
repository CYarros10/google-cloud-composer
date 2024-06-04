import json
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.sheets_to_gcs import (
    GoogleSheetsToGCSOperator,
)
from airflow.providers.google.suite.operators.sheets import (
    GoogleSheetsCreateSpreadsheetOperator,
)
from airflow.providers.google.suite.transfers.gcs_to_sheets import (
    GCSToGoogleSheetsOperator,
)
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_sheets_gcs"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
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
    description="This Airflow DAG creates a GCS bucket, interacts with a Google Sheet, transfers data between them, and then deletes both the sheet connection and bucket.",
    tags=["demo", "google_cloud", "gcs", "sheets"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    @task
    def create_temp_sheets_connection():
        conn = Connection(conn_id=CONNECTION_ID, conn_type="google_cloud_platform")
        conn_extra = {
            "scope": "https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/cloud-platform",
            "project": PROJECT_ID,
            "keyfile_dict": "",
        }
        conn_extra_json = json.dumps(conn_extra)
        conn.set_extra(conn_extra_json)
        session = Session()
        session.add(conn)
        session.commit()

    create_temp_sheets_connection_task = create_temp_sheets_connection()
    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET_NAME,
        spreadsheet_id="{{ task_instance.xcom_pull(task_ids='create_spreadsheet', key='spreadsheet_id') }}",
        gcp_conn_id=CONNECTION_ID,
    )
    create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
        task_id="create_spreadsheet", spreadsheet=SPREADSHEET, gcp_conn_id=CONNECTION_ID
    )
    print_spreadsheet_url = BashOperator(
        task_id="print_spreadsheet_url",
        bash_command=f"echo {XComArg(create_spreadsheet, key='spreadsheet_url')}",
    )
    upload_gcs_to_sheet = GCSToGoogleSheetsOperator(
        task_id="upload_gcs_to_sheet",
        bucket_name=BUCKET_NAME,
        object_name="{{ task_instance.xcom_pull('upload_sheet_to_gcs')[0] }}",
        spreadsheet_id="{{ task_instance.xcom_pull(task_ids='create_spreadsheet', key='spreadsheet_id') }}",
        gcp_conn_id=CONNECTION_ID,
    )
    delete_temp_sheets_connection_task = BashOperator(
        task_id="delete_temp_sheets_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        [create_bucket, create_temp_sheets_connection_task]
        >> create_spreadsheet
        >> print_spreadsheet_url
        >> upload_sheet_to_gcs
        >> upload_gcs_to_sheet
        >> [delete_bucket, delete_temp_sheets_connection_task]
    )
    upload_gcs_to_sheet >> end
