"""
Example Airflow DAG that starts, stops and sets the machine type of a Google Compute
Engine instance.

"""

from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineDeleteInstanceTemplateOperator,
    ComputeEngineInsertInstanceFromTemplateOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineInsertInstanceTemplateOperator,
    ComputeEngineSetMachineTypeOperator,
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_cloud_compute"
LOCATION_REGION = "us-central1"
ZONE = f"{LOCATION_REGION}-b"
GCE_INSTANCE_NAME = "instance-compute-test"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
TEMPLATE_NAME = "instance-template"
INSTANCE_TEMPLATE_BODY = {
    "name": TEMPLATE_NAME,
    "properties": {
        "machine_type": SHORT_MACHINE_TYPE_NAME,
        "disks": [
            {
                "auto_delete": True,
                "boot": True,
                "device_name": TEMPLATE_NAME,
                "initialize_params": {
                    "disk_size_gb": "10",
                    "disk_type": "pd-balanced",
                    "source_image": "projects/debian-cloud/global/images/debian-11-bullseye-v20220621",
                },
            }
        ],
        "network_interfaces": [{"network": "global/networks/default"}],
    },
}
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
GCE_INSTANCE_FROM_TEMPLATE_BODY = {"name": GCE_INSTANCE_NAME}
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
    description="This Airflow DAG creates, starts, stops, updates, and deletes a Google Compute Engine instance. The Airflow DAG covers both create and delete instance instances using GCE API, and both creating and deleting instance templates using GCE API.",
    tags=["demo", "google_cloud", "compute_engine"],
) as dag:
    gce_instance_insert = ComputeEngineInsertInstanceOperator(
        task_id="gcp_compute_create_instance_task",
        project_id=PROJECT_ID,
        zone=ZONE,
        body=GCE_INSTANCE_BODY,
    )
    gce_instance_insert2 = ComputeEngineInsertInstanceOperator(
        task_id="gcp_compute_create_instance_task_2",
        zone=ZONE,
        body=GCE_INSTANCE_BODY,
    )
    gce_instance_template_insert = ComputeEngineInsertInstanceTemplateOperator(
        task_id="gcp_compute_create_template_task",
        project_id=PROJECT_ID,
        body=INSTANCE_TEMPLATE_BODY,
    )
    gce_instance_template_insert2 = ComputeEngineInsertInstanceTemplateOperator(
        task_id="gcp_compute_create_template_task_2", body=INSTANCE_TEMPLATE_BODY
    )
    gce_instance_insert_from_template = ComputeEngineInsertInstanceFromTemplateOperator(
        task_id="gcp_compute_create_instance_from_template_task",
        project_id=PROJECT_ID,
        zone=ZONE,
        body=GCE_INSTANCE_FROM_TEMPLATE_BODY,
        source_instance_template=f"global/instanceTemplates/{TEMPLATE_NAME}",
    )
    gce_instance_insert_from_template2 = (
        ComputeEngineInsertInstanceFromTemplateOperator(
            task_id="gcp_compute_create_instance_from_template_task_2",
            zone=ZONE,
            body=GCE_INSTANCE_FROM_TEMPLATE_BODY,
            source_instance_template=f"global/instanceTemplates/{TEMPLATE_NAME}",
        )
    )
    gce_instance_start = ComputeEngineStartInstanceOperator(
        task_id="gcp_compute_start_task",
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
    )
    gce_instance_start2 = ComputeEngineStartInstanceOperator(
        task_id="gcp_compute_start_task_2", zone=ZONE, resource_id=GCE_INSTANCE_NAME
    )
    gce_instance_stop = ComputeEngineStopInstanceOperator(
        task_id="gcp_compute_stop_task",
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
    )
    gce_instance_stop.trigger_rule = TriggerRule.ALL_DONE
    gce_instance_stop2 = ComputeEngineStopInstanceOperator(
        task_id="gcp_compute_stop_task_2", zone=ZONE, resource_id=GCE_INSTANCE_NAME
    )
    gce_instance_stop2.trigger_rule = TriggerRule.ALL_DONE
    gce_set_machine_type = ComputeEngineSetMachineTypeOperator(
        task_id="gcp_compute_set_machine_type",
        project_id=PROJECT_ID,
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
        body={
            "machineType": f"zones/{ZONE}/machineTypes/{SHORT_MACHINE_TYPE_NAME}"
        },
    )
    gce_set_machine_type2 = ComputeEngineSetMachineTypeOperator(
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
        body={
            "machineType": f"zones/{ZONE}/machineTypes/{SHORT_MACHINE_TYPE_NAME}"
        },
        task_id="gcp_compute_set_machine_type_2",
    )
    gce_instance_delete = ComputeEngineDeleteInstanceOperator(
        task_id="gcp_compute_delete_instance_task",
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
    )
    gce_instance_delete.trigger_rule = TriggerRule.ALL_DONE
    gce_instance_delete2 = ComputeEngineDeleteInstanceOperator(
        task_id="gcp_compute_delete_instance_task_2",
        zone=ZONE,
        resource_id=GCE_INSTANCE_NAME,
    )
    gce_instance_delete.trigger_rule = TriggerRule.ALL_DONE
    gce_instance_template_delete = ComputeEngineDeleteInstanceTemplateOperator(
        task_id="gcp_compute_delete_template_task", resource_id=TEMPLATE_NAME
    )
    gce_instance_template_delete.trigger_rule = TriggerRule.ALL_DONE
    chain(
        gce_instance_insert,
        gce_instance_insert2,
        gce_instance_delete,
        gce_instance_template_insert,
        gce_instance_template_insert2,
        gce_instance_insert_from_template,
        gce_instance_insert_from_template2,
        gce_instance_start,
        gce_instance_start2,
        gce_instance_stop,
        gce_instance_stop2,
        gce_set_machine_type,
        gce_set_machine_type2,
        gce_instance_delete2,
        gce_instance_template_delete,
    )
