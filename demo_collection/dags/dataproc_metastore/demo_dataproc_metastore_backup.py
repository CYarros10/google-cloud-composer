"""
Airflow System Test DAG that verifies Dataproc Metastore
operators for managing backups.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreRestoreServiceOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dpms_backup"
ENV_ID = "composer"
PROJECT_ID = "your-project"
SERVICE_ID = f"{DAG_ID}-service-{ENV_ID}".replace("_", "-")
BACKUP_ID = f"{DAG_ID}-backup-{ENV_ID}".replace("_", "-")
LOCATION_REGION = "us-central1"
TIMEOUT = 2400
SERVICE = {"name": "test-service"}
BACKUP = {"name": "test-backup"}
with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=120),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=110),
    },
    description="This DAG creates and deletes a Dataproc Metastore service, creates a backup of the service, lists available backups, restores a service from a backup, and then deletes the backup and service.",
    tags=["demo", "google_cloud", "dataproc_metastore"],
) as dag:
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    backup_service = DataprocMetastoreCreateBackupOperator(
        task_id="create_backup",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        service_id=SERVICE_ID,
        backup=BACKUP,
        backup_id=BACKUP_ID,
        timeout=TIMEOUT,
    )
    list_backups = DataprocMetastoreListBackupsOperator(
        task_id="list_backups",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        service_id=SERVICE_ID,
    )
    delete_backup = DataprocMetastoreDeleteBackupOperator(
        task_id="delete_backup",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        service_id=SERVICE_ID,
        backup_id=BACKUP_ID,
        timeout=TIMEOUT,
    )
    delete_backup.trigger_rule = TriggerRule.ALL_DONE
    restore_service = DataprocMetastoreRestoreServiceOperator(
        task_id="restore_metastore",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        backup_id=BACKUP_ID,
        backup_region=LOCATION_REGION,
        backup_project_id=PROJECT_ID,
        backup_service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_service
        >> backup_service
        >> list_backups
        >> restore_service
        >> delete_backup
        >> delete_service
    )
