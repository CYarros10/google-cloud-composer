"""
Example Airflow DAG that show how to check Hive partitions existence
using Dataproc Metastore Sensor.

Note that Metastore service must be configured to use gRPC endpoints.
"""

from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.gcs import _parse_gcs_url
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteServiceOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataproc_metastore import (
    MetastoreHivePartitionSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dpms_hive_partition_sensor"
ENV_ID = "composer"
PROJECT_ID = "your-project"
LOCATION_REGION = "us-central1"
NETWORK = "default"
METASTORE_SERVICE_ID = f"metastore-{DAG_ID}-{ENV_ID}".replace("_", "-")
METASTORE_TIMEOUT = 2400
METASTORE_SERVICE = {
    "name": METASTORE_SERVICE_ID,
    "hive_metastore_config": {"endpoint_protocol": "GRPC"},
    "network": f"projects/{PROJECT_ID}/global/networks/{NETWORK}",
}
METASTORE_SERVICE_QFN = (
    f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/services/{METASTORE_SERVICE_ID}"
)
DATAPROC_CLUSTER_NAME = f"cluster-{DAG_ID}".replace("_", "-")
DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "metastore_config": {"dataproc_metastore_service": METASTORE_SERVICE_QFN},
    "gce_cluster_config": {
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    },
}
TABLE_NAME = "transactions_partitioned"
COLUMN = "TransactionType"
PARTITION_1 = f"{COLUMN}=credit".lower()
PARTITION_2 = f"{COLUMN}=debit".lower()
SOURCE_DATA_BUCKET = "airflow-system-tests-resources"
SOURCE_DATA_PATH = "dataproc/hive"
SOURCE_DATA_FILE_NAME = "part-00000.parquet"
EXTERNAL_TABLE_BUCKET = "{{task_instance.xcom_pull(task_ids='get_hive_warehouse_bucket_task', key='bucket')}}"
QUERY_CREATE_EXTERNAL_TABLE = f"\nCREATE EXTERNAL TABLE IF NOT EXISTS transactions\n(SubmissionDate DATE, TransactionAmount DOUBLE, TransactionType STRING)\nSTORED AS PARQUET\nLOCATION 'gs://{EXTERNAL_TABLE_BUCKET}/{SOURCE_DATA_PATH}';\n"
QUERY_CREATE_PARTITIONED_TABLE = f"\nCREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME}\n(SubmissionDate DATE, TransactionAmount DOUBLE)\nPARTITIONED BY ({COLUMN} STRING);\n"
QUERY_COPY_DATA_WITH_PARTITIONS = f"\nSET hive.exec.dynamic.partition.mode=nonstrict;\nINSERT INTO TABLE {TABLE_NAME} PARTITION ({COLUMN})\nSELECT SubmissionDate,TransactionAmount,TransactionType FROM transactions;\n"
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
    description=" This Airflow DAG waits for the creation of two Hive partitions in a table.\n",
    tags=["demo", "google_cloud", "dataproc_metastore"],
) as dag:
    create_metastore_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_metastore_service",
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
        service=METASTORE_SERVICE,
        service_id=METASTORE_SERVICE_ID,
        timeout=METASTORE_TIMEOUT,
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=DATAPROC_CLUSTER_NAME,
        project_id=PROJECT_ID,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
        region=LOCATION_REGION,
    )

    @task(task_id="get_hive_warehouse_bucket_task")
    def get_hive_warehouse_bucket(**kwargs):
        """Returns Hive Metastore Warehouse GCS bucket name."""
        ti = kwargs["ti"]
        metastore_service: dict = ti.xcom_pull(task_ids="create_metastore_service")
        config_overrides: dict = metastore_service["hive_metastore_config"][
            "config_overrides"
        ]
        destination_dir: str = config_overrides["hive.metastore.warehouse.dir"]
        bucket, _ = _parse_gcs_url(destination_dir)
        ti.xcom_push(key="bucket", value=bucket)

    get_hive_warehouse_bucket_task = get_hive_warehouse_bucket()
    copy_source_data = GCSToGCSOperator(
        task_id="copy_source_data",
        source_bucket=SOURCE_DATA_BUCKET,
        source_object=f"{SOURCE_DATA_PATH}/{SOURCE_DATA_FILE_NAME}",
        destination_bucket=EXTERNAL_TABLE_BUCKET,
        destination_object=f"{SOURCE_DATA_PATH}/{SOURCE_DATA_FILE_NAME}",
    )
    create_external_table = DataprocSubmitJobOperator(
        task_id="create_external_table",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "hive_job": {"query_list": {"queries": [QUERY_CREATE_EXTERNAL_TABLE]}},
        },
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    create_partitioned_table = DataprocSubmitJobOperator(
        task_id="create_partitioned_table",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "hive_job": {"query_list": {"queries": [QUERY_CREATE_PARTITIONED_TABLE]}},
        },
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    partition_data = DataprocSubmitJobOperator(
        task_id="partition_data",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "hive_job": {"query_list": {"queries": [QUERY_COPY_DATA_WITH_PARTITIONS]}},
        },
        region=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    hive_partition_sensor = MetastoreHivePartitionSensor(
        task_id="hive_partition_sensor",
        service_id=METASTORE_SERVICE_ID,
        region=LOCATION_REGION,
        table=TABLE_NAME,
        partitions=[PARTITION_1, PARTITION_2],
    )
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_name=DATAPROC_CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_metastore_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_metastore_service",
        service_id=METASTORE_SERVICE_ID,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_warehouse_bucket = GCSDeleteBucketOperator(
        task_id="delete_warehouse_bucket",
        bucket_name=EXTERNAL_TABLE_BUCKET,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        create_metastore_service
        >> create_cluster
        >> get_hive_warehouse_bucket_task
        >> copy_source_data
        >> create_external_table
        >> create_partitioned_table
        >> partition_data
    )
    (
        create_metastore_service
        >> hive_partition_sensor
        >> [delete_dataproc_cluster, delete_metastore_service, delete_warehouse_bucket]
    )
