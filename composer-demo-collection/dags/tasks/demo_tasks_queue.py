"""
Example Airflow DAG that creates, gets, lists, updates, purges, pauses, resumes
and deletes Queues in the Google Cloud Tasks service in the Google Cloud.

Required setup:
- GCP_APP_ENGINE_LOCATION: GCP Project's App Engine location `gcloud app describe | grep locationId`.
"""

from datetime import datetime, timedelta
from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue
from google.protobuf.field_mask_pb2 import FieldMask
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksQueueDeleteOperator,
    CloudTasksQueueGetOperator,
    CloudTasksQueuePauseOperator,
    CloudTasksQueuePurgeOperator,
    CloudTasksQueueResumeOperator,
    CloudTasksQueuesListOperator,
    CloudTasksQueueUpdateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"
DAG_ID = "demo_cloud_tasks_queue"
QUEUE_ID = f"queue-{ENV_ID}-{DAG_ID.replace('_', '-')}"
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
    description="This Airflow DAG demonstrates creating, managing, and deleting Cloud Tasks queues in Google Cloud.",
    tags=["demo", "google_cloud", "tasks"],
) as dag:

    @task(task_id="random_string")
    def generate_random_string():
        """
        Generate random string for queue and task names.
        Queue name cannot be repeated in preceding 7 days and
        task name in the last 1 hour.
        """
        import random
        import string

        return "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

    random_string = generate_random_string()
    create_queue = CloudTasksQueueCreateOperator(
        location=LOCATION_REGION,
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=0.5)),
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_queue",
    )
    delete_queue = CloudTasksQueueDeleteOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="delete_queue",
    )
    delete_queue.trigger_rule = TriggerRule.ALL_DONE
    resume_queue = CloudTasksQueueResumeOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="resume_queue",
    )
    pause_queue = CloudTasksQueuePauseOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="pause_queue",
    )
    purge_queue = CloudTasksQueuePurgeOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="purge_queue",
    )
    get_queue = CloudTasksQueueGetOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="get_queue",
    )
    get_queue_result = BashOperator(
        task_id="get_queue_result", bash_command=f"echo {get_queue.output}"
    )
    update_queue = CloudTasksQueueUpdateOperator(
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=1)),
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        update_mask=FieldMask(paths=["stackdriver_logging_config.sampling_ratio"]),
        task_id="update_queue",
    )
    list_queue = CloudTasksQueuesListOperator(
        location=LOCATION_REGION, task_id="list_queue"
    )
    chain(
        random_string,
        create_queue,
        update_queue,
        pause_queue,
        resume_queue,
        purge_queue,
        get_queue,
        get_queue_result,
        list_queue,
        delete_queue,
    )
