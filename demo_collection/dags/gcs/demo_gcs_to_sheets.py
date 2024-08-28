import json
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
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
PROJECT_ID = "your-project"
DAG_ID = "demo_gcs_to_sheets"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
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
    description="This DAG creates a GCS bucket, a new Google Sheet, uploads the sheet content to GCS, downloads from GCS to sheet, then finally deletes the bucket and the temporary connection.",
    tags=["demo", "google_cloud", "gcs"],
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
    create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
        task_id="create_spreadsheet", spreadsheet=SPREADSHEET, gcp_conn_id=CONNECTION_ID
    )
    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET_NAME,
        spreadsheet_id="{{ task_instance.xcom_pull(task_ids='create_spreadsheet', key='spreadsheet_id') }}",
        gcp_conn_id=CONNECTION_ID,
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
        >> upload_sheet_to_gcs
        >> upload_gcs_to_sheet
        >> [delete_bucket, delete_temp_sheets_connection_task]
    )
    upload_gcs_to_sheet >> end
