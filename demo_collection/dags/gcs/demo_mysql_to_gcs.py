"""
Example DAG using MySQLToGCSOperator.

This DAG relies on the following OS environment variables

* AIRFLOW__API__GOOGLE_KEY_PATH - Path to service account key file. Note, you can skip this variable if you
  run this DAG in a Composer environment.
"""

import logging
from datetime import datetime, timedelta
import pytest
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
except ImportError:
    pytest.skip("MySQL not available", allow_module_level=True)
ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_mysql_to_gcs"
REGION = "europe-west2"
ZONE = REGION + "-a"
NETWORK = "default"
DB_NAME = "testdb"
DB_PORT = 3306
DB_USER_NAME = "root"
DB_USER_PASSWORD = "demo_password"
SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
DB_INSTANCE_NAME = f"instance-{DAG_ID}-{ENV_ID}".replace("_", "-")
GCE_INSTANCE_BODY = {
    "name": DB_INSTANCE_NAME,
    "machine_type": f"zones/{ZONE}/machineTypes/{SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "device_name": DB_INSTANCE_NAME,
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
            "subnetwork": f"regions/{REGION}/subnetworks/default",
        }
    ],
}
DELETE_PERSISTENT_DISK = f"\nif [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then  gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; fi;\n\ngcloud compute disks delete {DB_INSTANCE_NAME} --project={PROJECT_ID} --zone={ZONE} --quiet\n"
SETUP_MYSQL = f"\nsudo apt update &&\nsudo apt install -y docker.io &&\nsudo docker run -d -p {DB_PORT}:{DB_PORT} --name {DB_NAME}     -e MYSQL_ROOT_PASSWORD={DB_USER_PASSWORD}     -e MYSQL_DATABASE={DB_NAME}     mysql:8.1.0\n"
FIREWALL_RULE_NAME = f"allow-http-{DB_PORT}"
CREATE_FIREWALL_RULE = f"\nif [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then  gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; fi;\n\ngcloud compute firewall-rules create {FIREWALL_RULE_NAME}   --project={PROJECT_ID}   --direction=INGRESS   --priority=100   --network={NETWORK}   --action=ALLOW   --rules=tcp:{DB_PORT}   --source-ranges=0.0.0.0/0\n"
DELETE_FIREWALL_RULE = f"\nif [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then  gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; fi;\n\ngcloud compute firewall-rules delete {FIREWALL_RULE_NAME} --project={PROJECT_ID} --quiet\n"
CONNECTION_ID = f"mysql_{DAG_ID}_{ENV_ID}".replace("-", "_")
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "result.json"
SQL_TABLE = "test_table"
SQL_CREATE = f"CREATE TABLE IF NOT EXISTS {SQL_TABLE} (col_1 INT, col_2 VARCHAR(8))"
SQL_INSERT = f"INSERT INTO {SQL_TABLE} (col_1, col_2) VALUES (1, 'one'), (2, 'two')"
SQL_SELECT = f"SELECT * FROM {SQL_TABLE}"
log = logging.getLogger(__name__)
with DAG(
    dag_id=DAG_ID,
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
    description="This DAG orchestrates the creation of a MySQL database, exports data to GCS, and cleans up resources afterwards.",
    tags=["demo", "google_cloud", "gcs", "mysql"],
) as dag:
    create_instance = ComputeEngineInsertInstanceOperator(
        task_id="create_instance",
        project_id=PROJECT_ID,
        zone=ZONE,
        body=GCE_INSTANCE_BODY,
    )
    create_firewall_rule = BashOperator(
        task_id="create_firewall_rule", bash_command=CREATE_FIREWALL_RULE
    )
    setup_mysql = SSHOperator(
        task_id="setup_mysql",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name=DB_INSTANCE_NAME,
            zone=ZONE,
            project_id=PROJECT_ID,
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=180,
        ),
        command=SETUP_MYSQL,
        retries=2,
    )

    @task
    def get_public_ip() -> str:
        hook = ComputeEngineHook()
        address = hook.get_instance_address(
            resource_id=DB_INSTANCE_NAME, zone=ZONE, project_id=PROJECT_ID
        )
        return address

    get_public_ip_task = get_public_ip()

    @task
    def setup_mysql_connection(**kwargs) -> None:
        public_ip = kwargs["ti"].xcom_pull(task_ids="get_public_ip")
        connection = Connection(
            conn_id=CONNECTION_ID,
            description="Example MySQL connection",
            conn_type="mysql",
            host=public_ip,
            login=DB_USER_NAME,
            password=DB_USER_PASSWORD,
            schema=DB_NAME,
        )
        session = Session()
        if (
            session.query(Connection)
            .filter(Connection.conn_id == CONNECTION_ID)
            .first()
        ):
            log.warning("Connection %s already exists", CONNECTION_ID)
            return None
        session.add(connection)
        session.commit()

    setup_mysql_connection_task = setup_mysql_connection()
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )
    create_sql_table = SQLExecuteQueryOperator(
        task_id="create_sql_table", conn_id=CONNECTION_ID, sql=SQL_CREATE
    )
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data", conn_id=CONNECTION_ID, sql=SQL_INSERT
    )
    upload_mysql_to_gcs = MySQLToGCSOperator(
        task_id="mysql_to_gcs",
        sql=SQL_SELECT,
        bucket=BUCKET_NAME,
        filename=FILE_NAME,
        export_format="csv",
    )
    delete_mysql_connection = BashOperator(
        task_id="delete_mysql_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_instance = ComputeEngineDeleteInstanceOperator(
        task_id="delete_instance",
        resource_id=DB_INSTANCE_NAME,
        zone=ZONE,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_firewall_rule = BashOperator(
        task_id="delete_firewall_rule",
        bash_command=DELETE_FIREWALL_RULE,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_persistent_disk = BashOperator(
        task_id="delete_persistent_disk",
        bash_command=DELETE_PERSISTENT_DISK,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        create_instance
        >> setup_mysql
        >> get_public_ip_task
        >> setup_mysql_connection_task
        >> create_firewall_rule
        >> create_sql_table
        >> insert_data
    )
    (
        [insert_data, create_bucket]
        >> upload_mysql_to_gcs
        >> [
            delete_instance,
            delete_bucket,
            delete_mysql_connection,
            delete_firewall_rule,
        ]
    )
    delete_instance >> delete_persistent_disk
    upload_mysql_to_gcs >> end
