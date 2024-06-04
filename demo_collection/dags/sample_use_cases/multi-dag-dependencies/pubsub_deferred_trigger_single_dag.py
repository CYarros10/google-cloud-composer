from datetime import datetime, timedelta

from airflow import models
from airflow import XComArg
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import (
    PubSubPullSensor
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
    f"pubsub_deferred_trigger_single_dag_{VERSION}",
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

    pull_messages_sensor = PubSubPullSensor(
        task_id="pull_messages_sensor",
        project_id=PROJECT,
        ack_messages=True,
        messages_callback=handle_messages,
        subscription=subscription,
        max_messages=50,
        mode="reschedule",
        poke_interval=180
    )

    trigger_target_dag = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id=f"target_dag_{VERSION}"
    )

    subscribe_task >> pull_messages_sensor >> trigger_target_dag