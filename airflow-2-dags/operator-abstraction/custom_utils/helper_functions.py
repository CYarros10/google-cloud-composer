from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)

# -------------------------
# Callback Functions / Custom Operators
# -------------------------
class CustomDataprocCreateClusterOperator(DataprocCreateClusterOperator):
    """
    standardizes what can be set for cluster creation
    forces a specific cluster config
    """
    def __init__(self, project_id: str, region: str, cluster_name: str, **kwargs):
        super().__init__(project_id=project_id, region=region, cluster_name=cluster_name, **kwargs)
        self.cluster_config = {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 1024,
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 1024,
                },
            },
    }

def setup_cluster_taskgroup(group_id, project_id, cluster_name, region):
    """
    Delete (if exists) and create a dataproc cluster.
    """
    with TaskGroup(group_id=group_id) as dataproc_tg1:

        pre_delete_cluster = DataprocDeleteClusterOperator(
            task_id="pre_delete_cluster",
            project_id=project_id,
            cluster_name=cluster_name,
            region=region,
        )

        create_cluster = CustomDataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=project_id,
            cluster_name=cluster_name,
            region=region,
            trigger_rule="all_done",
        )
        pre_delete_cluster >> create_cluster

    return dataproc_tg1
