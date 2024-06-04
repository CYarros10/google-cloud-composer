from datetime import datetime, timedelta

from airflow import models
from airflow import XComArg
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubPullOperator
)

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_1"
PROJECT = ""
TOPIC_ID = "multi-dag-test"
SUBSCRIPTION = "trigger_dag_subscription"

#-------------------------
# Tags, Default Args, and Macros
#-------------------------
tags = [
    f"project:{PROJECT}",
]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023,8,1),
    "sla": timedelta(minutes=30)
}

def handle_messages(pulled_messages, context):
    dag_ids = list()
    for idx, m in enumerate(pulled_messages):
        data = m.message.data.decode("utf-8")
        print(f"message {idx} data is {data}")
        dag_ids.append(data)
    return dag_ids


# This DAG will run minutely and handle pub/sub messages by triggering target DAG
with models.DAG(
    f"pubsub_trigger_dag_list_{VERSION}",
    start_date=datetime(2023, 8, 1),
    schedule_interval="* * * * *",
    max_active_runs=1,
    catchup=False,
) as trigger_dag:
    
    # If subscription exists, we will use it. If not - create new one
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe_task",
        project_id=PROJECT,
        topic=TOPIC_ID,
        subscription=SUBSCRIPTION,
    )

    subscription = subscribe_task.output

    # Proceed maximum 50 messages in callback function handle_messages
    # Here we acknowledge messages automatically. You can use PubSubHook.acknowledge to acknowledge in downstream tasks
    # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/hooks/pubsub/index.html#airflow.providers.google.cloud.hooks.pubsub.PubSubHook.acknowledge
    pull_messages_operator = PubSubPullOperator(
        task_id="pull_messages_operator",
        project_id=PROJECT,
        ack_messages=True,
        messages_callback=handle_messages,
        subscription=subscription,
        max_messages=50,
    )

    # Here we use Dynamic Task Mapping to trigger DAGs according to messages content
    # https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html
    trigger_target_dag = TriggerDagRunOperator.partial(task_id="trigger_target").expand(
        trigger_dag_id=XComArg(pull_messages_operator)
    )

    subscribe_task >> pull_messages_operator >> trigger_target_dag