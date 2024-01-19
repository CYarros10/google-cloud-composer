"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models

# ----- Google Cloud Storage Airflow Imports
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectUpdateSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSUploadSessionCompleteSensor
)

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_1"

# -------------------------
# Tags, Default Args, and Macros
# -------------------------
tags = ["application:samples"]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2022, 3, 15),
    "mode": "poke", # default to poke unless otherwise specified
    "poke_interval": 50, # default to 50 second poke interval unless otherwise specified
    "sla": timedelta(minutes=25),
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"gcp_sensors_dag_{VERSION}",
    description="Sample DAG for various sensor tasks.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    gcs_bucket_name = ""

    # Poke vs. Reschedule vs. Deferrable
    
    # 1. Continuously check for the existence of a specific object.
    # Uses the MOST Airflow Worker resources, fastest sense time
    continuous_check_file_exists = GCSObjectExistenceSensor(
        task_id="continuous_check_file_exists",
        bucket=gcs_bucket_name,
        object="sensor-demo/continuous.txt",
        mode="poke", # poke will allocate airflow worker slot to this task until it is complete
        poke_interval=10, # mode = poke should be used for sensors checking on a per-seconds basis
        timeout=600 # timeout after 10 minutes
    )

    # 2. Periodically check for the existence of a specific object.
    # Uses less Airflow Worker resources, slower sense time
    periodic_check_file_exists = GCSObjectExistenceSensor(
        task_id="periodic_check_file_exists",
        bucket=gcs_bucket_name,
        object="sensor-demo/periodic.txt",
        mode="reschedule", # reschedule will free up airflow worker slot during the poke_interval
        poke_interval=60, # check every 1 minutes. mode = reschedule should be used for sensors checking on a per-minutes basis
        timeout=1800 # timeout after 30 minutes
    )

    # 3. Intelligently and periodically check for existence of specific object based on available airflow resources.
    # Uses the least Airflow Worker resources, offloads to Airflow Triggerer, slowest sense time
    # DEFERRABLE REQUIRES TRIGGERER TO BE ENABLED IN COMPOSER ENVIRONMENT
    # https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html#difference-between-mode-reschedule-and-deferrable-true-in-sensors
    deferrable_check_file_exists = GCSObjectExistenceSensor(
        task_id="deferrable_check_file_exists",
        bucket=gcs_bucket_name,
        object="sensor-demo/deferrable.txt",
        deferrable=True, # using deferrable operators for sensor tasks can provide efficiency gains and reduce operational costs by offloading work to Triggerer
        poke_interval=60, # check every 1 minute. deferrable=True should be used for sensors checking on a per-minutes-to-hour basis
        timeout=1800, # timeout after 30 minutes
    )    

    # 4. Check for the existence of a specific object via GLOB:
    check_file_glob_exists = GCSObjectExistenceSensor(
        task_id="check_file_glob_exists",
        bucket=gcs_bucket_name,
        object="sensor-demo/glob*",
        use_glob=True # REQUIRES apache-airflow-provides-google>=10.13.1
    )

    # 5. Check for the existence of objects with a prefix.
    #Checks for the existence of GCS objects at a given prefix, passing matches via XCom.
    # matching objects will be passed through XCom for downstream tasks.
    check_prefix_exists = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_prefix_exists",
        bucket=gcs_bucket_name,
        prefix="sensor-demo/prefix/",
        mode="poke",
        poke_interval=30 # Check status every 60 seconds  
    )

    # 6. Check to see if an object is updated:
    wait_for_update = GCSObjectUpdateSensor(
        task_id="wait_for_update",
        bucket=gcs_bucket_name,
        object="sensor-demo/prefix/update.txt"
        # The default callback returns execution_date + schedule_interval. The callback takes the context as parameter.
    )

    # 7. Checks for changes in the number of objects at prefix in Google Cloud Storage bucket
    #    returns True if the inactivity period has passed with no increase in the number of objects
    # This sensor will not behave correctly in reschedule mode
    check_upload_complete = GCSUploadSessionCompleteSensor(
        task_id = "check_upload_complete",
        bucket=gcs_bucket_name,
        prefix="sensor-demo/",
        inactivity_period=60, # no new objects for 60 seconds 
        mode="poke",
        poke_interval=30 # Check status every 30 seconds  
    )
    
    (
        continuous_check_file_exists 
        >> periodic_check_file_exists
        >> deferrable_check_file_exists
        >> check_file_glob_exists
        >> check_prefix_exists
        >> wait_for_update
        >> check_upload_complete
    )
