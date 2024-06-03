"""
Example Airflow DAG for Google Cloud Memorystore service.
"""

from datetime import datetime, timedelta
from google.cloud.redis_v1 import FailoverInstanceRequest, Instance
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_memorystore import (
    CloudMemorystoreCreateInstanceAndImportOperator,
    CloudMemorystoreCreateInstanceOperator,
    CloudMemorystoreDeleteInstanceOperator,
    CloudMemorystoreExportAndDeleteInstanceOperator,
    CloudMemorystoreExportInstanceOperator,
    CloudMemorystoreFailoverInstanceOperator,
    CloudMemorystoreGetInstanceOperator,
    CloudMemorystoreImportOperator,
    CloudMemorystoreListInstancesOperator,
    CloudMemorystoreScaleInstanceOperator,
    CloudMemorystoreUpdateInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
ENV_ID_LOWER = ENV_ID.lower() if ENV_ID else ""
DAG_ID = "demo_cloud_memorystore_redis"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
LOCATION_REGION = "us-central1"
MEMORYSTORE_REDIS_INSTANCE_NAME = f"redis-{ENV_ID_LOWER}-1"
MEMORYSTORE_REDIS_INSTANCE_NAME_2 = f"redis-{ENV_ID_LOWER}-2"
MEMORYSTORE_REDIS_INSTANCE_NAME_3 = f"redis-{ENV_ID_LOWER}-3"
EXPORT_GCS_URL = f"gs://{BUCKET_NAME}/my-export.rdb"
FIRST_INSTANCE = {"tier": Instance.Tier.BASIC, "memory_size_gb": 1}
SECOND_INSTANCE = {"tier": Instance.Tier.STANDARD_HA, "memory_size_gb": 3}
with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
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
    description=" This Airflow DAG creates, manages, and deletes Google Cloud Memorystore instances for Redis.",
    tags=["demo", "google_cloud", "bigquery", "redis"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        resource={"predefined_acl": "public_read_write"},
    )
    create_instance = CloudMemorystoreCreateInstanceOperator(
        task_id="create-instance",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME,
        instance=FIRST_INSTANCE,
        project_id=PROJECT_ID,
    )
    create_instance_result = BashOperator(
        task_id="create-instance-result", bash_command=f"echo {create_instance.output}"
    )
    create_instance_2 = CloudMemorystoreCreateInstanceOperator(
        task_id="create-instance-2",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        instance=SECOND_INSTANCE,
        project_id=PROJECT_ID,
    )
    get_instance = CloudMemorystoreGetInstanceOperator(
        task_id="get-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME,
        project_id=PROJECT_ID,
        do_xcom_push=True,
    )
    get_instance_result = BashOperator(
        task_id="get-instance-result", bash_command=f"echo {get_instance.output}"
    )
    failover_instance = CloudMemorystoreFailoverInstanceOperator(
        task_id="failover-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        data_protection_mode=FailoverInstanceRequest.DataProtectionMode(
            FailoverInstanceRequest.DataProtectionMode.LIMITED_DATA_LOSS
        ),
        project_id=PROJECT_ID,
    )
    list_instances = CloudMemorystoreListInstancesOperator(
        task_id="list-instances", location="-", page_size=100, project_id=PROJECT_ID
    )
    list_instances_result = BashOperator(
        task_id="list-instances-result", bash_command=f"echo {get_instance.output}"
    )
    update_instance = CloudMemorystoreUpdateInstanceOperator(
        task_id="update-instance",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME,
        project_id=PROJECT_ID,
        update_mask={"paths": ["memory_size_gb"]},
        instance={"memory_size_gb": 2},
    )
    set_acl_permission = GCSBucketCreateAclEntryOperator(
        task_id="gcs-set-acl-permission",
        bucket=BUCKET_NAME,
        entity="user-{{ task_instance.xcom_pull('get-instance')['persistence_iam_identity'].split(':', 2)[1] }}",
        role="OWNER",
    )
    export_instance = CloudMemorystoreExportInstanceOperator(
        task_id="export-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME,
        output_config={"gcs_destination": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    import_instance = CloudMemorystoreImportOperator(
        task_id="import-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        input_config={"gcs_source": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    delete_instance = CloudMemorystoreDeleteInstanceOperator(
        task_id="delete-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME,
        project_id=PROJECT_ID,
    )
    delete_instance.trigger_rule = TriggerRule.ALL_DONE
    delete_instance_2 = CloudMemorystoreDeleteInstanceOperator(
        task_id="delete-instance-2",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_2,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    create_instance_and_import = CloudMemorystoreCreateInstanceAndImportOperator(
        task_id="create-instance-and-import",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME_3,
        instance=FIRST_INSTANCE,
        input_config={"gcs_source": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    scale_instance = CloudMemorystoreScaleInstanceOperator(
        task_id="scale-instance",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_REDIS_INSTANCE_NAME_3,
        project_id=PROJECT_ID,
        memory_size_gb=3,
    )
    export_and_delete_instance = CloudMemorystoreExportAndDeleteInstanceOperator(
        task_id="export-and-delete-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_REDIS_INSTANCE_NAME_3,
        output_config={"gcs_destination": {"uri": EXPORT_GCS_URL}},
        project_id=PROJECT_ID,
    )
    export_and_delete_instance.trigger_rule = TriggerRule.ALL_DONE
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_bucket
        >> create_instance
        >> create_instance_result
        >> get_instance
        >> get_instance_result
        >> set_acl_permission
        >> export_instance
        >> update_instance
        >> list_instances
        >> list_instances_result
        >> create_instance_2
        >> failover_instance
        >> import_instance
        >> delete_instance
        >> delete_instance_2
        >> create_instance_and_import
        >> scale_instance
        >> export_and_delete_instance
        >> delete_bucket
    )
