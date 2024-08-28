"""
Example Airflow DAG that:
* creates a copy of existing Instance Template
* updates existing template in Instance Group Manager

"""

from __future__ import annotations
import os
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineCopyInstanceTemplateOperator,
    ComputeEngineDeleteInstanceGroupManagerOperator,
    ComputeEngineDeleteInstanceTemplateOperator,
    ComputeEngineInsertInstanceGroupManagerOperator,
    ComputeEngineInsertInstanceTemplateOperator,
    ComputeEngineInstanceGroupUpdateManagerTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_cloud_compute_igm"
LOCATION_REGION = "us-central1"
ZONE = f"{LOCATION_REGION}-b"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
TEMPLATE_NAME = "instance-template-igm-test"
NEW_TEMPLATE_NAME = "instance-template-test-new"
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
NEW_DESCRIPTION = "Test new description"
INSTANCE_TEMPLATE_BODY_UPDATE = {
    "name": NEW_TEMPLATE_NAME,
    "description": NEW_DESCRIPTION,
    "properties": {"machine_type": "n1-standard-2"},
}
INSTANCE_GROUP_MANAGER_NAME = "instance-group-test"
INSTANCE_GROUP_MANAGER_BODY = {
    "name": INSTANCE_GROUP_MANAGER_NAME,
    "base_instance_name": INSTANCE_GROUP_MANAGER_NAME,
    "instance_template": f"global/instanceTemplates/{TEMPLATE_NAME}",
    "target_size": 1,
}
SOURCE_TEMPLATE_URL = f"https://www.googleapis.com/compute/beta/projects/{PROJECT_ID}/global/instanceTemplates/{TEMPLATE_NAME}"
DESTINATION_TEMPLATE_URL = f"https://www.googleapis.com/compute/beta/projects/{PROJECT_ID}/global/instanceTemplates/{NEW_TEMPLATE_NAME}"
UPDATE_POLICY = {
    "type": "OPPORTUNISTIC",
    "minimalAction": "RESTART",
    "maxSurge": {"fixed": 1},
    "minReadySec": 1800,
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
    description="This Airflow DAG creates a copy of an existing Instance Template and updates an existing template in an Instance Group Manager.",
    tags=["demo", "google_cloud", "compute_engine"],
) as dag:
    gce_instance_template_insert = ComputeEngineInsertInstanceTemplateOperator(
        task_id="gcp_compute_create_template_task",
        project_id=PROJECT_ID,
        body=INSTANCE_TEMPLATE_BODY,
    )
    gce_instance_template_insert2 = ComputeEngineInsertInstanceTemplateOperator(
        task_id="gcp_compute_create_template_task_2", body=INSTANCE_TEMPLATE_BODY
    )
    gce_instance_template_copy = ComputeEngineCopyInstanceTemplateOperator(
        task_id="gcp_compute_igm_copy_template_task",
        project_id=PROJECT_ID,
        resource_id=TEMPLATE_NAME,
        body_patch=INSTANCE_TEMPLATE_BODY_UPDATE,
    )
    gce_instance_template_copy2 = ComputeEngineCopyInstanceTemplateOperator(
        task_id="gcp_compute_igm_copy_template_task_2",
        resource_id=TEMPLATE_NAME,
        body_patch=INSTANCE_TEMPLATE_BODY_UPDATE,
    )
    gce_igm_insert = ComputeEngineInsertInstanceGroupManagerOperator(
        task_id="gcp_compute_create_group_task",
        zone=ZONE,
        body=INSTANCE_GROUP_MANAGER_BODY,
        project_id=PROJECT_ID,
    )
    gce_igm_insert2 = ComputeEngineInsertInstanceGroupManagerOperator(
        task_id="gcp_compute_create_group_task_2",
        zone=ZONE,
        body=INSTANCE_GROUP_MANAGER_BODY,
    )
    gce_instance_group_manager_update_template = (
        ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            task_id="gcp_compute_igm_group_manager_update_template",
            project_id=PROJECT_ID,
            resource_id=INSTANCE_GROUP_MANAGER_NAME,
            zone=ZONE,
            source_template=SOURCE_TEMPLATE_URL,
            destination_template=DESTINATION_TEMPLATE_URL,
            update_policy=UPDATE_POLICY,
        )
    )
    gce_instance_group_manager_update_template2 = (
        ComputeEngineInstanceGroupUpdateManagerTemplateOperator(
            task_id="gcp_compute_igm_group_manager_update_template_2",
            resource_id=INSTANCE_GROUP_MANAGER_NAME,
            zone=ZONE,
            source_template=SOURCE_TEMPLATE_URL,
            destination_template=DESTINATION_TEMPLATE_URL,
        )
    )
    gce_instance_template_old_delete = ComputeEngineDeleteInstanceTemplateOperator(
        task_id="gcp_compute_delete_old_template_task", resource_id=TEMPLATE_NAME
    )
    gce_instance_template_old_delete.trigger_rule = TriggerRule.ALL_DONE
    gce_instance_template_new_delete = ComputeEngineDeleteInstanceTemplateOperator(
        task_id="gcp_compute_delete_new_template_task", resource_id=NEW_TEMPLATE_NAME
    )
    gce_instance_template_new_delete.trigger_rule = TriggerRule.ALL_DONE
    gce_igm_delete = ComputeEngineDeleteInstanceGroupManagerOperator(
        task_id="gcp_compute_delete_group_task",
        resource_id=INSTANCE_GROUP_MANAGER_NAME,
        zone=ZONE,
    )
    gce_igm_delete.trigger_rule = TriggerRule.ALL_DONE
    chain(
        gce_instance_template_insert,
        gce_instance_template_insert2,
        gce_instance_template_copy,
        gce_instance_template_copy2,
        gce_igm_insert,
        gce_igm_insert2,
        gce_instance_group_manager_update_template,
        gce_instance_group_manager_update_template2,
        gce_igm_delete,
        gce_instance_template_old_delete,
        gce_instance_template_new_delete,
    )
