"""
Example Airflow DAG that creates, patches and deletes a Cloud SQL instance, and also
creates, patches and deletes a database inside the instance, in Google Cloud.

"""

from datetime import datetime, timedelta
from urllib.parse import urlsplit
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCloneInstanceOperator,
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceDatabaseOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExportInstanceOperator,
    CloudSQLImportInstanceOperator,
    CloudSQLInstancePatchOperator,
    CloudSQLPatchInstanceDatabaseOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_cloudsql"
LOCATION_REGION="us-central1"
INSTANCE_NAME = f"{DAG_ID}-{ENV_ID}-instance".replace("_", "-")
DB_NAME = f"{DAG_ID}-{ENV_ID}-db".replace("_", "-")
BUCKET_NAME = f"{DAG_ID}_{ENV_ID}_bucket".replace("-", "_")
FILE_NAME = f"{DAG_ID}_{ENV_ID}_exportImportTestFile".replace("-", "_")
FILE_NAME_DEFERRABLE = f"{DAG_ID}_{ENV_ID}_def_exportImportTestFile".replace("-", "_")
FILE_URI = f"gs://{BUCKET_NAME}/{FILE_NAME}"
FILE_URI_DEFERRABLE = f"gs://{BUCKET_NAME}/{FILE_NAME_DEFERRABLE}"
FAILOVER_REPLICA_NAME = f"{INSTANCE_NAME}-failover-replica"
READ_REPLICA_NAME = f"{INSTANCE_NAME}-read-replica"
CLONED_INSTANCE_NAME = f"{INSTANCE_NAME}-clone"
body = {
    "name": INSTANCE_NAME,
    "settings": {
        "tier": "db-n1-standard-1",
        "backupConfiguration": {
            "binaryLogEnabled": True,
            "enabled": True,
            "startTime": "05:00",
        },
        "activationPolicy": "ALWAYS",
        "dataDiskSizeGb": 30,
        "dataDiskType": "PD_SSD",
        "databaseFlags": [],
        "ipConfiguration": {"ipv4Enabled": True, "requireSsl": True},
        "locationPreference": {"zone": f"{LOCATION_REGION}-a"},
        "maintenanceWindow": {"hour": 5, "day": 7, "updateTrack": "canary"},
        "pricingPlan": "PER_USE",
        "replicationType": "ASYNCHRONOUS",
        "storageAutoResize": True,
        "storageAutoResizeLimit": 0,
        "userLabels": {"my-key": "my-value"},
    },
    "failoverReplica": {"name": FAILOVER_REPLICA_NAME},
    "databaseVersion": "MYSQL_5_7",
    "region": LOCATION_REGION,
}
read_replica_body = {
    "name": READ_REPLICA_NAME,
    "settings": {"tier": "db-n1-standard-1"},
    "databaseVersion": "MYSQL_5_7",
    "region": LOCATION_REGION,
    "masterInstanceName": INSTANCE_NAME,
}
patch_body = {
    "name": INSTANCE_NAME,
    "settings": {
        "dataDiskSizeGb": 35,
        "maintenanceWindow": {"hour": 3, "day": 6, "updateTrack": "canary"},
        "userLabels": {"my-key-patch": "my-value-patch"},
    },
}
export_body = {
    "exportContext": {
        "fileType": "sql",
        "uri": FILE_URI,
        "sqlExportOptions": {"schemaOnly": False},
        "offload": True,
    }
}
export_body_deferrable = {
    "exportContext": {
        "fileType": "sql",
        "uri": FILE_URI_DEFERRABLE,
        "sqlExportOptions": {"schemaOnly": False},
        "offload": True,
    }
}
import_body = {"importContext": {"fileType": "sql", "uri": FILE_URI}}
db_create_body = {"instance": INSTANCE_NAME, "name": DB_NAME, "project": PROJECT_ID}
db_patch_body = {"charset": "utf16", "collation": "utf16_general_ci"}
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
    description="This DAG creates, modifies, clones, and then deletes an instance and its database, while also performing exports and imports using the Cloudsql service.",
    tags=["demo", "google_cloud", "gcloud_sql"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        resource={"predefined_acl": "public_read_write"},
    )
    sql_instance_create_task = CloudSQLCreateInstanceOperator(
        body=body, instance=INSTANCE_NAME, task_id="sql_instance_create_task"
    )
    sql_instance_read_replica_create = CloudSQLCreateInstanceOperator(
        body=read_replica_body,
        instance=READ_REPLICA_NAME,
        task_id="sql_instance_read_replica_create",
    )
    sql_instance_patch_task = CloudSQLInstancePatchOperator(
        body=patch_body, instance=INSTANCE_NAME, task_id="sql_instance_patch_task"
    )
    sql_db_create_task = CloudSQLCreateInstanceDatabaseOperator(
        body=db_create_body, instance=INSTANCE_NAME, task_id="sql_db_create_task"
    )
    sql_db_patch_task = CloudSQLPatchInstanceDatabaseOperator(
        body=db_patch_body,
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id="sql_db_patch_task",
    )
    file_url_split = urlsplit(FILE_URI)
    service_account_email = XComArg(
        sql_instance_create_task, key="service_account_email"
    )
    sql_gcp_add_bucket_permission_task = GCSBucketCreateAclEntryOperator(
        entity=f"user-{service_account_email}",
        role="WRITER",
        bucket=file_url_split[1],
        task_id="sql_gcp_add_bucket_permission_task",
    )
    sql_export_task = CloudSQLExportInstanceOperator(
        body=export_body, instance=INSTANCE_NAME, task_id="sql_export_task"
    )
    sql_export_def_task = CloudSQLExportInstanceOperator(
        body=export_body_deferrable,
        instance=INSTANCE_NAME,
        task_id="sql_export_def_task",
        deferrable=True,
    )
    sql_gcp_add_object_permission_task = GCSObjectCreateAclEntryOperator(
        entity=f"user-{service_account_email}",
        role="READER",
        bucket=file_url_split[1],
        object_name=file_url_split[2][1:],
        task_id="sql_gcp_add_object_permission_task",
    )
    sql_import_task = CloudSQLImportInstanceOperator(
        body=import_body, instance=INSTANCE_NAME, task_id="sql_import_task"
    )
    sql_instance_clone = CloudSQLCloneInstanceOperator(
        instance=INSTANCE_NAME,
        destination_instance_name=CLONED_INSTANCE_NAME,
        task_id="sql_instance_clone",
    )
    sql_db_delete_task = CloudSQLDeleteInstanceDatabaseOperator(
        instance=INSTANCE_NAME,
        database=DB_NAME,
        task_id="sql_db_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    sql_instance_failover_replica_delete_task = CloudSQLDeleteInstanceOperator(
        instance=FAILOVER_REPLICA_NAME,
        task_id="sql_instance_failover_replica_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    sql_instance_read_replica_delete_task = CloudSQLDeleteInstanceOperator(
        instance=READ_REPLICA_NAME,
        task_id="sql_instance_read_replica_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    sql_instance_clone_delete_task = CloudSQLDeleteInstanceOperator(
        instance=CLONED_INSTANCE_NAME,
        task_id="sql_instance_clone_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    sql_instance_delete_task = CloudSQLDeleteInstanceOperator(
        instance=INSTANCE_NAME,
        task_id="sql_instance_delete_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> sql_instance_create_task
        >> sql_instance_read_replica_create
        >> sql_instance_patch_task
        >> sql_db_create_task
        >> sql_db_patch_task
        >> sql_gcp_add_bucket_permission_task
        >> sql_export_task
        >> sql_export_def_task
        >> sql_gcp_add_object_permission_task
        >> sql_import_task
        >> sql_instance_clone
        >> sql_db_delete_task
        >> sql_instance_failover_replica_delete_task
        >> sql_instance_read_replica_delete_task
        >> sql_instance_clone_delete_task
        >> sql_instance_delete_task
        >> delete_bucket
    )
