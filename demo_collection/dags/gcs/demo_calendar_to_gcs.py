import json
import os
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.calendar_to_gcs import (
    GoogleCalendarToGCSOperator,
)
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_calendar_to_gcs"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CALENDAR_ID = os.environ.get("CALENDAR_ID", "primary")
API_VERSION = "v3"
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
    description="This DAG automatically retrieves events from a Google Calendar into a GCS bucket, then cleans up.",
    tags=["demo", "google_cloud", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    @task
    def create_temp_gcp_connection():
        conn = Connection(conn_id=CONNECTION_ID, conn_type="google_cloud_platform")
        conn_extra = {
            "scope": "https://www.googleapis.com/auth/calendar",
            "project": PROJECT_ID,
            "keyfile_dict": "",
        }
        conn_extra_json = json.dumps(conn_extra)
        conn.set_extra(conn_extra_json)
        session = Session()
        session.add(conn)
        session.commit()

    create_temp_gcp_connection_task = create_temp_gcp_connection()
    upload_calendar_to_gcs = GoogleCalendarToGCSOperator(
        task_id="upload_calendar_to_gcs",
        destination_bucket=BUCKET_NAME,
        calendar_id=CALENDAR_ID,
        api_version=API_VERSION,
        gcp_conn_id=CONNECTION_ID,
    )
    delete_temp_gcp_connection_task = BashOperator(
        task_id="delete_temp_gcp_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_bucket
        >> create_temp_gcp_connection_task
        >> upload_calendar_to_gcs
        >> delete_temp_gcp_connection_task
        >> delete_bucket
    )
    delete_temp_gcp_connection_task >> end
