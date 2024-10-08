import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import (
    GoogleDriveToGCSOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.suite.transfers.gcs_to_gdrive import (
    GCSToGoogleDriveOperator,
)
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_gdrive_to_gcs_with_gdrive_sensor"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
OBJECT = "abc123xyz"
FOLDER_ID = ""
FILE_NAME = "example_upload.txt"
DRIVE_FILE_NAME = f"example_upload_{DAG_ID}_{ENV_ID}.txt"
LOCAL_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
log = logging.getLogger(__name__)
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
    description=" Airflow DAG demonstrating file transfers between GCS and Google Drive.\n",
    tags=["demo", "google_cloud", "gcs", "drive"],
) as dag:

    @task
    def create_temp_gcp_connection():
        conn = Connection(conn_id=CONNECTION_ID, conn_type="google_cloud_platform")
        conn_extra_json = json.dumps(
            {
                "scope": "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"
            }
        )
        conn.set_extra(conn_extra_json)
        session = Session()
        if (
            session.query(Connection)
            .filter(Connection.conn_id == CONNECTION_ID)
            .first()
        ):
            log.warning("Connection %s already exists", CONNECTION_ID)
            return None
        session.add(conn)
        session.commit()

    create_temp_gcp_connection_task = create_temp_gcp_connection()
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file", src=LOCAL_PATH, dst=FILE_NAME, bucket=BUCKET_NAME
    )
    copy_single_file = GCSToGoogleDriveOperator(
        task_id="copy_single_file",
        gcp_conn_id=CONNECTION_ID,
        source_bucket=BUCKET_NAME,
        source_object=FILE_NAME,
        destination_object=DRIVE_FILE_NAME,
    )
    detect_file = GoogleDriveFileExistenceSensor(
        task_id="detect_file",
        folder_id=FOLDER_ID,
        file_name=DRIVE_FILE_NAME,
        gcp_conn_id=CONNECTION_ID,
    )
    upload_gdrive_to_gcs = GoogleDriveToGCSOperator(
        task_id="upload_gdrive_object_to_gcs",
        gcp_conn_id=CONNECTION_ID,
        folder_id=FOLDER_ID,
        file_name=DRIVE_FILE_NAME,
        bucket_name=BUCKET_NAME,
        object_name=OBJECT,
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def remove_files_from_drive():
        service = GoogleDriveHook(gcp_conn_id=CONNECTION_ID).get_conn()
        response = service.files().list(q=f"name = '{DRIVE_FILE_NAME}'").execute()
        if files := response["files"]:
            file = files[0]
            log.info("Deleting file {}...", file)
            service.files().delete(fileId=file["id"])
            log.info("Done.")

    remove_files_from_drive_task = remove_files_from_drive()
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_temp_gcp_connection_task = BashOperator(
        task_id="delete_temp_gcp_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        [
            create_bucket >> upload_file >> copy_single_file,
            create_temp_gcp_connection_task,
        ]
        >> detect_file
        >> upload_gdrive_to_gcs
        >> remove_files_from_drive_task
        >> [delete_bucket, delete_temp_gcp_connection_task]
    )
    remove_files_from_drive_task >> end
