"""
Example Airflow DAG for Google Cloud Memorystore Memcached service.

This DAG relies on the following OS environment variables

* AIRFLOW__API__GOOGLE_KEY_PATH - Path to service account key file. Note, you can skip this variable if you
  run this DAG in a Composer environment.
"""

from datetime import datetime, timedelta
from google.protobuf.field_mask_pb2 import FieldMask
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_memorystore import (
    CloudMemorystoreMemcachedApplyParametersOperator,
    CloudMemorystoreMemcachedCreateInstanceOperator,
    CloudMemorystoreMemcachedDeleteInstanceOperator,
    CloudMemorystoreMemcachedGetInstanceOperator,
    CloudMemorystoreMemcachedListInstancesOperator,
    CloudMemorystoreMemcachedUpdateInstanceOperator,
    CloudMemorystoreMemcachedUpdateParametersOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_cloud_memorystore_memcached"
MEMORYSTORE_MEMCACHED_INSTANCE_NAME = f"memcached-{ENV_ID}-1"
LOCATION_REGION = "us-central1"
MEMCACHED_INSTANCE = {
    "name": "",
    "node_count": 1,
    "node_config": {"cpu_count": 1, "memory_size_mb": 1024},
    "zones": [LOCATION_REGION + "-a"],
}
IP_RANGE_NAME = f"ip-range-{DAG_ID}-{ENV_ID}".replace("_", "-")
NETWORK = "default"
CREATE_PRIVATE_CONNECTION_CMD = f"""\nif [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then  gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; fi;\nif [[ $(gcloud compute addresses list --project={PROJECT_ID} --filter="name=('{IP_RANGE_NAME}')") ]]; then   echo "The IP range '{IP_RANGE_NAME}' already exists in the project '{PROJECT_ID}'."; else   echo "  Creating IP range...";   gcloud compute addresses create "{IP_RANGE_NAME}"     --global     --purpose=VPC_PEERING     --prefix-length=16     --description="IP range for Memorystore system tests"     --network={NETWORK}     --project={PROJECT_ID};   echo "Done."; fi;\nif [[ $(gcloud services vpc-peerings list --network={NETWORK} --project={PROJECT_ID}) ]]; then   echo "The private connection already exists in the project '{PROJECT_ID}'."; else   echo "  Creating private connection...";   gcloud services vpc-peerings connect     --service=servicenetworking.googleapis.com     --ranges={IP_RANGE_NAME}     --network={NETWORK}     --project={PROJECT_ID};   echo "Done."; fi;\nif [[ $(gcloud services vpc-peerings list         --network={NETWORK}         --project={PROJECT_ID}         --format="value(reservedPeeringRanges)" | grep {IP_RANGE_NAME}) ]]; then   echo "Private service connection configured."; else   echo "  Updating service private connection...";   gcloud services vpc-peerings update     --service=servicenetworking.googleapis.com     --ranges={IP_RANGE_NAME}     --network={NETWORK}     --project={PROJECT_ID}     --force; fi;\n"""
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
    description="This Airflow DAG demonstrates the creation, management, and deletion of a Cloud Memorystore for Memcached instance.",
    tags=["demo", "google_cloud", "memorystore", "memcached"],
) as dag:
    create_private_service_connection = BashOperator(
        task_id="create_private_service_connection",
        bash_command=CREATE_PRIVATE_CONNECTION_CMD,
    )
    create_memcached_instance = CloudMemorystoreMemcachedCreateInstanceOperator(
        task_id="create-instance",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        instance=MEMCACHED_INSTANCE,
        project_id=PROJECT_ID,
    )
    delete_memcached_instance = CloudMemorystoreMemcachedDeleteInstanceOperator(
        task_id="delete-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
    )
    delete_memcached_instance.trigger_rule = TriggerRule.ALL_DONE
    get_memcached_instance = CloudMemorystoreMemcachedGetInstanceOperator(
        task_id="get-instance",
        location=LOCATION_REGION,
        instance=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
    )
    list_memcached_instances = CloudMemorystoreMemcachedListInstancesOperator(
        task_id="list-instances", location="-", project_id=PROJECT_ID
    )
    update_memcached_instance = CloudMemorystoreMemcachedUpdateInstanceOperator(
        task_id="update-instance",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
        update_mask=FieldMask(paths=["node_count"]),
        instance={"node_count": 2},
    )
    update_memcached_parameters = CloudMemorystoreMemcachedUpdateParametersOperator(
        task_id="update-parameters",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
        update_mask={"paths": ["params"]},
        parameters={"params": {"protocol": "ascii", "hash_algorithm": "jenkins"}},
    )
    apply_memcached_parameters = CloudMemorystoreMemcachedApplyParametersOperator(
        task_id="apply-parameters",
        location=LOCATION_REGION,
        instance_id=MEMORYSTORE_MEMCACHED_INSTANCE_NAME,
        project_id=PROJECT_ID,
        node_ids=["node-a-1"],
        apply_all=False,
    )
    (
        create_private_service_connection
        >> create_memcached_instance
        >> get_memcached_instance
        >> list_memcached_instances
        >> update_memcached_instance
        >> update_memcached_parameters
        >> apply_memcached_parameters
        >> delete_memcached_instance
    )
