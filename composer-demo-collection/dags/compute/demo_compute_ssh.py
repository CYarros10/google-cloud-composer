"""
Example Airflow DAG that starts, stops and sets the machine type of a Google Compute
Engine instance.

"""

from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_cloud_compute_ssh"
LOCATION_REGION = "us-central1"
ZONE = f"{LOCATION_REGION}-b"
GCE_INSTANCE_NAME = "instance-ssh-test"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
GCE_INSTANCE_BODY = {
    "name": GCE_INSTANCE_NAME,
    "machine_type": f"zones/{ZONE}/machineTypes/{SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "device_name": GCE_INSTANCE_NAME,
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{ZONE}/diskTypes/pd-balanced",
                "source_image": "projects/debian-cloud/global/images/debian-11-bullseye-v20220621",
            },
        }
    ],
    "network_interfaces": [
        {
            "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            "stack_type": "IPV4_ONLY",
            "subnetwork": f"regions/{LOCATION_REGION}/subnetworks/default",
        }
    ],
}
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
    description="Starts, stops, and then deletes a Google Cloud Compute Engine instance.",
    tags=["demo", "google_cloud", "compute_engine", "ssh"],
) as dag:
    gce_instance_insert = ComputeEngineInsertInstanceOperator(
        task_id="gcp_compute_create_instance_task",
        project_id=PROJECT_ID,
        zone=ZONE,
        body=GCE_INSTANCE_BODY,
    )
    metadata_without_iap_tunnel1 = SSHOperator(
        task_id="metadata_without_iap_tunnel1",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name=GCE_INSTANCE_NAME,
            zone=ZONE,
            project_id=PROJECT_ID,
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=1,
        ),
        command="echo metadata_without_iap_tunnel1",
    )
    metadata_without_iap_tunnel2 = SSHOperator(
        task_id="metadata_without_iap_tunnel2",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name=GCE_INSTANCE_NAME,
            zone=ZONE,
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=100,
        ),
        command="echo metadata_without_iap_tunnel2",
    )
    gce_instance_delete = ComputeEngineDeleteInstanceOperator(
        task_id="gcp_compute_delete_instance_task",
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
    )
    gce_instance_delete.trigger_rule = TriggerRule.ALL_DONE
    chain(
        gce_instance_insert,
        metadata_without_iap_tunnel1,
        metadata_without_iap_tunnel2,
        gce_instance_delete,
    )
