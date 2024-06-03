"""
Example Airflow DAG that creates and deletes Queues and creates, gets, lists,
runs and deletes Tasks in the Google Cloud Tasks service in the Google Cloud.
"""

from datetime import datetime, timedelta
from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue
from google.protobuf import timestamp_pb2
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksQueueDeleteOperator,
    CloudTasksTaskCreateOperator,
    CloudTasksTaskDeleteOperator,
    CloudTasksTaskGetOperator,
    CloudTasksTaskRunOperator,
    CloudTasksTasksListOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"
DAG_ID = "demo_cloud_tasks"
timestamp = timestamp_pb2.Timestamp()
timestamp.FromDatetime(datetime.now() + timedelta(hours=12))
QUEUE_ID = f"queue-{ENV_ID}-{DAG_ID.replace('_', '-')}"
TASK_NAME = "task-to-run"
TASK = {
    "http_request": {
        "http_method": "POST",
        "url": "http://www.example.com/example",
        "body": b"",
    },
    "schedule_time": timestamp,
}
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
    description="This Airflow DAG creates and deletes Queues and creates, gets, lists, runs and deletes Tasks in the Google Cloud Tasks service in the Google Cloud.",
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
    create_task = CloudTasksTaskCreateOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task=TASK,
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_task_to_run",
    )
    tasks_get = CloudTasksTaskGetOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="tasks_get",
    )
    run_task = CloudTasksTaskRunOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        task_id="run_task",
    )
    list_tasks = CloudTasksTasksListOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="list_tasks",
    )
    delete_task = CloudTasksTaskDeleteOperator(
        location=LOCATION_REGION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="delete_task",
    )
    chain(
        random_string,
        create_queue,
        create_task,
        tasks_get,
        list_tasks,
        run_task,
        delete_task,
        delete_queue,
    )
