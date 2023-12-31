"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

import time
from datetime import timedelta, datetime
from airflow import models

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
)

timestr = time.strftime("%Y%m%d-%H%M%S")


# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

# -------------------------
# Tags, Default Args, and
# Macros
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
    "start_date": datetime(2023, 8, 17),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}

user_defined_macros = {
    "project_id": "",
    "region": "us-central1",
    "dp_cluster_name": "health-check-cluster",
    "gcs_output_location": f"gs://bucket/hadoop/{timestr}",
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"dataproc_workflow_dag_{VERSION}",
    description="Sample DAG for Dataproc Workflow Template usage.",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    template_id = "wft_demo_1"
    wft = {
        "id": template_id,
        "jobs": [
            {
                "step_id": "teragen",
                "hadoop_job": {
                    "args": ["teragen", "1000", "hdfs:///gen/"],
                    "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
                },
            },
            {
                "step_id": "terasort",
                "hadoop_job": {
                    "args": ["terasort", "hdfs:///gen/", "hdfs:///sort/"],
                    "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
                },
                "prerequisite_step_ids": ["teragen"],
            },
            {
                "step_id": "hive_demo",
                "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
                "prerequisite_step_ids": ["terasort"],
            },
            {
                "step_id": "pig_demo",
                "pig_job": {"query_list": {"queries": ["define sin HiveUDF('sin');"]}},
                "prerequisite_step_ids": ["hive_demo"],
            },
            {
                "step_id": "spark_demo",
                "spark_job": {
                    "jar_file_uris": [
                        "file:///usr/lib/spark/examples/jars/spark-examples.jar"
                    ],
                    "main_class": "org.apache.spark.examples.SparkPi",
                },
                "prerequisite_step_ids": ["pig_demo"],
            },
        ],
        "placement": {
            "managed_cluster": {
                "cluster_name": "{{dp_cluster_name}}",
                "config": {
                    "gce_cluster_config": {"zone_uri": ""},
                    "master_config": {
                        "disk_config": {
                            "boot_disk_size_gb": 100,
                            "boot_disk_type": "pd-standard",
                        },
                        "machine_type_uri": "n2-standard-2",
                        "min_cpu_platform": "AUTOMATIC",
                        "num_instances": 1,
                        "preemptibility": "NON_PREEMPTIBLE",
                    },
                    "worker_config": {
                        "disk_config": {
                            "boot_disk_size_gb": 100,
                            "boot_disk_type": "pd-standard",
                        },
                        "machine_type_uri": "n2-standard-2",
                        "min_cpu_platform": "AUTOMATIC",
                        "num_instances": 2,
                        "preemptibility": "NON_PREEMPTIBLE",
                    },
                },
            }
        },
    }

    # project_id and region are not templated ...
    # this will not replace existing workflow templates (need to be deleted first)
    create_workflow_template = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template",
        template=wft,
        project_id="",
        region="us-central1",
    )

    trigger_workflow_async = DataprocInstantiateWorkflowTemplateOperator(
        task_id="trigger_workflow_async",
        project_id="",
        region="us-central1",
        template_id=template_id,
        deferrable=True,
    )

    # will show as "None" in the Workflows console
    instantiate_inline_workflow_template_async = (
        DataprocInstantiateInlineWorkflowTemplateOperator(
            task_id="instantiate_inline_workflow_template_async",
            template=wft,
            project_id="",
            region="us-central1",
            deferrable=True,
        )
    )

    (
        create_workflow_template
        >> trigger_workflow_async
        >> instantiate_inline_workflow_template_async
    )
