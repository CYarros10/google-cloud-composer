"""
A DAG in a collection of samples for getting started with
Google Cloud services, or running proof-of-concepts, demos,
etc. on Cloud Composer.
"""

from datetime import timedelta, datetime
from airflow import models

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)

# ---------------------
# Universal DAG info
# ---------------------
VERSION = "v0_0_0"

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
    "start_date": datetime(2023, 8, 17),
    "mode": "reschedule",
    "poke_interval": 60,
    "use_legacy_sql": False,
    "sla": timedelta(minutes=25),
}

# -------------------------
# Begin DAG Generation
# -------------------------
with models.DAG(
    f"hadoop_streaming_demo_{VERSION}",
    description="example hadoop streaming dag",
    schedule="0 0 * * *",  # midnight daily
    tags=tags,
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=False,
    max_active_runs=2,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    project_id=""
    region=""
    cluster_name=""
    bucket=""

    # equivalent gcloud command
    # gcloud dataproc jobs submit hadoop --cluster cluster-4db0 --region us-central1 --jar file:///usr/lib/hadoop/hadoop-streaming.jar --files gs://cy-sandbox/hadoop_streaming_sample/mapper.py,gs://cy-sandbox/hadoop_streaming_sample/reducer.py -- -mapper mapper.py -reducer reducer.py -input gs://cy-sandbox/hadoop_streaming_sample/data.txt  -output gs://cy-sandbox/hadoop_streaming_sample/output

    hadoop_streaming_py_job = DataprocSubmitJobOperator(
        task_id="hadoop_streaming_py_job",
        project_id=project_id,
        region=region,
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "hadoop_job": {
                "main_jar_file_uri": "file:///usr/lib/hadoop/hadoop-streaming.jar",
                "file_uris": [
                    f"gs://{bucket}/hadoop_streaming_sample/python/mapper.py",
                    f"gs://{bucket}/hadoop_streaming_sample/python/reducer.py"
                ],
                "args": [
                    "-mapper",
                    "mapper.py",
                    "-reducer",
                    "reducer.py",
                    "-input",
                    f"gs://{bucket}/hadoop_streaming_sample/python/data.txt",
                    "-output",
                    f"gs://{bucket}/hadoop_streaming_sample/python/output"
                ],
            },
        }
    )

    hadoop_streaming_pl_job = DataprocSubmitJobOperator(
        task_id="hadoop_streaming_pl_job",
        project_id=project_id,
        region=region,
        job={
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "hadoop_job": {
                "main_jar_file_uri": "file:///usr/lib/hadoop/hadoop-streaming.jar",
                "file_uris": [
                    "gs://cy-sandbox/hadoop_streaming_sample/perl/mapper.pl",
                    "gs://cy-sandbox/hadoop_streaming_sample/perl/reducer.pl"
                ],
                "args": [
                    "-mapper",
                    "mapper.pl",
                    "-reducer",
                    "reducer.pl",
                    "-input",
                    "gs://cy-sandbox/hadoop_streaming_sample/perl/data.txt",
                    "-output",
                    "gs://cy-sandbox/hadoop_streaming_sample/perl/output"
                ],
            },
        }
    )