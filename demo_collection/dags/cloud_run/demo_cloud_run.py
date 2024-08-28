"""
Example Airflow DAG that uses Google Cloud Run Operators.
"""

from datetime import datetime, timedelta
from google.cloud.run_v2 import Job
from google.cloud.run_v2.types import k8s_min
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunDeleteJobOperator,
    CloudRunExecuteJobOperator,
    CloudRunListJobsOperator,
    CloudRunUpdateJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_cloud_run"
LOCATION_REGION = "us-central1"
job_name_prefix = "cloudrun-system-test-job"
job1_name = f"{job_name_prefix}1"
job2_name = f"{job_name_prefix}2"

create1_task_name = "create-job1"
create2_task_name = "create-job2"
execute1_task_name = "execute-job1"
execute2_task_name = "execute-job2"

update_job1_task_name = "update-job1"
delete1_task_name = "delete-job1"
delete2_task_name = "delete-job2"
list_jobs_limit_task_name = "list-jobs-limit"
list_jobs_task_name = "list-jobs"
clean1_task_name = "clean-job1"
clean2_task_name = "clean-job2"


def _assert_executed_jobs_xcom(ti):
    job1_dicts = ti.xcom_pull(task_ids=[execute1_task_name], key="return_value")
    assert job1_name in job1_dicts[0]["name"]
    job2_dicts = ti.xcom_pull(task_ids=[execute2_task_name], key="return_value")
    assert job2_name in job2_dicts[0]["name"]


def _assert_created_jobs_xcom(ti):
    job1_dicts = ti.xcom_pull(task_ids=[create1_task_name], key="return_value")
    assert job1_name in job1_dicts[0]["name"]
    job2_dicts = ti.xcom_pull(task_ids=[create2_task_name], key="return_value")
    assert job2_name in job2_dicts[0]["name"]


def _assert_updated_job(ti):
    job_dicts = ti.xcom_pull(task_ids=[update_job1_task_name], key="return_value")
    job_dict = job_dicts[0]
    assert job_dict["labels"]["somelabel"] == "label1"


def _assert_jobs(ti):
    job_dicts = ti.xcom_pull(task_ids=[list_jobs_task_name], key="return_value")
    job1_exists = False
    job2_exists = False
    for job_dict in job_dicts[0]:
        if job1_exists and job2_exists:
            break
        if job1_name in job_dict["name"]:
            job1_exists = True
        if job2_name in job_dict["name"]:
            job2_exists = True
    assert job1_exists and job2_exists


def _assert_one_job(ti):
    job_dicts = ti.xcom_pull(task_ids=[list_jobs_limit_task_name], key="return_value")
    assert len(job_dicts[0]) == 1


def _create_job():
    job = Job()
    container = k8s_min.Container()
    container.image = "us-docker.pkg.dev/cloudrun/container/job:latest"
    job.template.template.containers.append(container)
    return job


def _create_job_with_label():
    job = _create_job()
    job.labels = {"somelabel": "label1"}
    return job


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
    description="This Airflow DAG shows how to create and manage Cloud Run jobs.",
    tags=["demo", "google_cloud", "cloud_run"],
) as dag:
    create1 = CloudRunCreateJobOperator(
        task_id=create1_task_name,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job1_name,
        job=_create_job(),
        dag=dag,
    )
    create2 = CloudRunCreateJobOperator(
        task_id=create2_task_name,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job2_name,
        job=Job.to_dict(_create_job()),
        dag=dag,
    )
    assert_created_jobs = PythonOperator(
        task_id="assert-created-jobs",
        python_callable=_assert_created_jobs_xcom,
        dag=dag,
    )
    execute1 = CloudRunExecuteJobOperator(
        task_id=execute1_task_name,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job1_name,
        dag=dag,
        deferrable=False,
    )
    execute2 = CloudRunExecuteJobOperator(
        task_id=execute2_task_name,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job2_name,
        dag=dag,
        deferrable=True,
    )
    overrides = {
        "container_overrides": [
            {
                "name": "job",
                "args": ["python", "main.py"],
                "env": [{"name": "ENV_VAR", "value": "value"}]
            }
        ],
        "task_count": 1,
        "timeout": "60s",
    }
    assert_executed_jobs = PythonOperator(
        task_id="assert-executed-jobs",
        python_callable=_assert_executed_jobs_xcom,
        dag=dag,
    )
    list_jobs_limit = CloudRunListJobsOperator(
        task_id=list_jobs_limit_task_name,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        dag=dag,
        limit=1,
    )
    assert_jobs_limit = PythonOperator(
        task_id="assert-jobs-limit", python_callable=_assert_one_job, dag=dag
    )
    list_jobs = CloudRunListJobsOperator(
        task_id=list_jobs_task_name, project_id=PROJECT_ID, region=LOCATION_REGION, dag=dag
    )
    assert_jobs = PythonOperator(
        task_id="assert-jobs", python_callable=_assert_jobs, dag=dag
    )
    update_job1 = CloudRunUpdateJobOperator(
        task_id=update_job1_task_name,
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job1_name,
        job=_create_job_with_label(),
        dag=dag,
    )
    assert_job_updated = PythonOperator(
        task_id="assert-job-updated", python_callable=_assert_updated_job, dag=dag
    )
    delete_job1 = CloudRunDeleteJobOperator(
        task_id="delete-job1",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job1_name,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_job2 = CloudRunDeleteJobOperator(
        task_id="delete-job2",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        job_name=job2_name,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        (create1, create2)
        >> assert_created_jobs
        >> (execute1, execute2)
        >> assert_executed_jobs
        >> list_jobs_limit
        >> assert_jobs_limit
        >> list_jobs
        >> assert_jobs
        >> update_job1
        >> assert_job_updated
        >> (delete_job1, delete_job2)
    )
