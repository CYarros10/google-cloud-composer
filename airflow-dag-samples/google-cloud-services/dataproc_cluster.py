# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Examples below show how to use dataproc operators
"""

from datetime import timedelta, datetime
from airflow import models
import time

# ----- Dataproc Imports
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import (
    DataprocJobSensor
)


#---------------------
# Universal DAG info
#---------------------
VERSION = "v1_0_0"
PROJECT = ""
BUNDLE="demo"
COMPOSER_ID="demo"
ORG="googlecloud"


#-------------------------
# Tags, Default Args, and Macros
#-------------------------
tags = [
    f"bundle:{BUNDLE}",
    f"project:{PROJECT}",
    f"org:{ORG}",
    f"composer_id:{COMPOSER_ID}"
]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 8, 17),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=1)
}
timestr = time.strftime("%Y%m%d-%H%M%S")
user_defined_macros = {
    "project_id": "",
    "region": "us-central1",
    "dp_cluster_name": "health-check-cluster",
    "gcs_output_location": f"gs://<bucket>/hadoop/{timestr}"
}

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"dataproc_demo_{VERSION}",
    description="example dataproc dag",
    schedule_interval="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    

    pre_delete_cluster = DataprocDeleteClusterOperator(
        task_id="pre_delete_cluster",
        project_id="{{project_id}}",
        cluster_name="{{dp_cluster_name}}",
        region="{{region}}",
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id="{{project_id}}",
        region="{{region}}",
        cluster_name="{{dp_cluster_name}}",
        cluster_config={
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
        },
        trigger_rule="all_done",
    )

    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_cluster",
        project_id="{{project_id}}",
        region="{{region}}",
        cluster_name="{{dp_cluster_name}}",
        graceful_decommission_timeout={"seconds": 600},
        cluster={
            "config": {
                "worker_config": {"num_instances": 3},
                "secondary_worker_config": {"num_instances": 3},
            }
        },
        update_mask={
            "paths": [
                "config.worker_config.num_instances",
                "config.secondary_worker_config.num_instances",
            ]
        },
        deferrable=True,
    )

    HIVE_JOB_CONFIG = {
        "reference": {"project_id": "{{project_id}}"},
        "placement": {"cluster_name": "{{dp_cluster_name}}"},
        "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}}
    }

    hive_job = DataprocSubmitJobOperator(
        task_id="hive_job",
        job=HIVE_JOB_CONFIG,
        region="{{region}}",
        project_id="{{project_id}}",
    )

    PIG_JOB_CONFIG = {
        "reference": {"project_id": "{{project_id}}"},
        "placement": {"cluster_name": "{{dp_cluster_name}}"},
        "pig_job": {"query_list": {"queries": ["define sin HiveUDF('sin');"]}},
    }

    pig_job = DataprocSubmitJobOperator(
        task_id="pig_job",
        job=PIG_JOB_CONFIG,
        region="{{region}}",
        project_id="{{project_id}}",
    )

    HADOOP_JOB_CONFIG = {
        "reference": {"project_id": "{{project_id}}"},
        "placement": {"cluster_name": "{{dp_cluster_name}}"},
        "hadoop_job": {
            "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
            "args": ["wordcount", "gs://pub/shakespeare/rose.txt", "{{gcs_output_location}}"],
        },
    }

    hadoop_job = DataprocSubmitJobOperator(
        task_id="hadoop_job",
        job=HADOOP_JOB_CONFIG,
        region="{{region}}",
        project_id="{{project_id}}",
    )

    SPARK_JOB_CONFIG = {
        "reference": {"project_id": "{{project_id}}"},
        "placement": {"cluster_name": "{{dp_cluster_name}}"},
        "spark_job": {
            "jar_file_uris": [
                "file:///usr/lib/spark/examples/jars/spark-examples.jar"
            ],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
    }

    spark_job_async = DataprocSubmitJobOperator(
        task_id="spark_job_async",
        job=SPARK_JOB_CONFIG,
        region="{{region}}",
        project_id="{{project_id}}",
        asynchronous=True
    )

    spark_job_async_sensor = DataprocJobSensor(
        task_id='spark_task_async_sensor_task',
        region="{{region}}",
        project_id="{{project_id}}",
        dataproc_job_id="{{ ti.xcom_pull('spark_job_async') }}",
    )

    post_delete_cluster = DataprocDeleteClusterOperator(
        task_id="post_delete_cluster",
        project_id="{{project_id}}",
        cluster_name="{{dp_cluster_name}}",
        region="{{region}}",
        trigger_rule="all_done",
    )

    pre_delete_cluster >> create_cluster >> scale_cluster >> [spark_job_async_sensor,hive_job,pig_job,hadoop_job] >> post_delete_cluster
    scale_cluster >> spark_job_async >> spark_job_async_sensor