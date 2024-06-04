"""
Example DAG using GoogleCloudStorageToGoogleDriveOperator.

Using this operator requires the following additional scopes:
https://www.googleapis.com/auth/drive
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.transfers.gcs_to_gdrive import (
    GCSToGoogleDriveOperator,
)
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
FOLDER_ID = os.environ.get("GCP_GDRIVE_FOLDER_ID", "root")
DAG_ID = "demo_gcs_to_gdrive"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
TMP_PATH = "tmp"
WORK_DIR = f"folder_{DAG_ID}_{ENV_ID}".replace("-", "_")
CURRENT_FOLDER = Path(__file__).parent
LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources")
FILE_LOCAL_PATH = str(Path(LOCAL_PATH))
FILE_NAME = "example_upload.txt"
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
    description="The DAG transfers single/multiple files from Google Cloud Storage to Google Drive and moves/deletes them.",
    tags=["demo", "google_cloud", "gcs"],
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
    upload_file_1 = LocalFilesystemToGCSOperator(
        task_id="upload_file_1",
        src=f"{FILE_LOCAL_PATH}/{FILE_NAME}",
        dst=f"{TMP_PATH}/{FILE_NAME}",
        bucket=BUCKET_NAME,
    )
    upload_file_2 = LocalFilesystemToGCSOperator(
        task_id="upload_file_2",
        src=f"{FILE_LOCAL_PATH}/{FILE_NAME}",
        dst=f"{TMP_PATH}/2_{FILE_NAME}",
        bucket=BUCKET_NAME,
    )
    copy_single_file = GCSToGoogleDriveOperator(
        task_id="copy_single_file",
        gcp_conn_id=CONNECTION_ID,
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/{FILE_NAME}",
        destination_object=f"{WORK_DIR}/copied_{FILE_NAME}",
    )
    copy_single_file_into_folder = GCSToGoogleDriveOperator(
        task_id="copy_single_file_into_folder",
        gcp_conn_id=CONNECTION_ID,
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/{FILE_NAME}",
        destination_object=f"{WORK_DIR}/copied_{FILE_NAME}",
        destination_folder_id=FOLDER_ID,
    )
    copy_files = GCSToGoogleDriveOperator(
        task_id="copy_files",
        gcp_conn_id=CONNECTION_ID,
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/*",
        destination_object=f"{WORK_DIR}/",
    )
    move_files = GCSToGoogleDriveOperator(
        task_id="move_files",
        gcp_conn_id=CONNECTION_ID,
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/*.txt",
        destination_object=f"{WORK_DIR}/",
        move_object=True,
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def remove_files_from_drive():
        service = GoogleDriveHook(gcp_conn_id=CONNECTION_ID).get_conn()
        root_path = (
            service.files()
            .list(
                q=f"name = '{WORK_DIR}' and mimeType = 'application/vnd.google-apps.folder'"
            )
            .execute()
        )
        if files := root_path["files"]:
            batch = service.new_batch_http_request()
            for file in files:
                log.info("Preparing to remove file: {}", file)
                batch.add(service.files().delete(fileId=file["id"]))
            batch.execute()
            log.info("Selected files removed.")

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
    create_bucket >> [upload_file_1, upload_file_2]
    (
        [upload_file_1, upload_file_2, create_temp_gcp_connection_task]
        >> copy_single_file
        >> copy_single_file_into_folder
        >> copy_files
        >> move_files
        >> remove_files_from_drive_task
        >> [delete_bucket, delete_temp_gcp_connection_task]
    )
    remove_files_from_drive_task >> end
