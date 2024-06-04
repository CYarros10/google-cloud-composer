"""
Example Airflow DAG for Google Cloud Storage to SFTP transfer operators.
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_sftp_to_gcs"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"
TMP_PATH = "tmp"
DIR = "tests_sftp_hook_dir"
SUBDIR = "subdir"
OBJECT_SRC_1 = "parent-1.bin"
OBJECT_SRC_2 = "parent-2.bin"
CURRENT_FOLDER = Path(__file__).parent
LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources")
FILE_LOCAL_PATH = str(Path(LOCAL_PATH) / TMP_PATH / DIR)
FILE_NAME = "tmp.tar.gz"
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
    description="This Airflow DAG transfers files and directories from SFTP to GCS.",
    tags=["demo", "google_cloud", "gcs", "sftp"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    unzip_file = BashOperator(
        task_id="unzip_data_file",
        bash_command=f"tar xvf {LOCAL_PATH}/{FILE_NAME} -C {LOCAL_PATH}",
    )
    copy_file_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="file-copy-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{OBJECT_SRC_1}",
        destination_bucket=BUCKET_NAME,
    )
    move_file_from_sftp_to_gcs_destination = SFTPToGCSOperator(
        task_id="file-move-sftp-to-gcs-destination",
        source_path=f"{FILE_LOCAL_PATH}/{OBJECT_SRC_2}",
        destination_bucket=BUCKET_NAME,
        destination_path="destination_dir/destination_filename.bin",
        move_object=True,
    )
    copy_directory_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="dir-copy-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{SUBDIR}/*",
        destination_bucket=BUCKET_NAME,
    )
    move_specific_files_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="dir-move-specific-files-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{SUBDIR}/*.bin",
        destination_bucket=BUCKET_NAME,
        destination_path="specific_files/",
        move_object=True,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    chain(
        create_bucket,
        unzip_file,
        copy_file_from_sftp_to_gcs,
        move_file_from_sftp_to_gcs_destination,
        copy_directory_from_sftp_to_gcs,
        move_specific_files_from_sftp_to_gcs,
        delete_bucket,
    )
    move_specific_files_from_sftp_to_gcs >> end
