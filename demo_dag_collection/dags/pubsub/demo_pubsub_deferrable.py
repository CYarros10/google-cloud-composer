"""
Example Airflow DAG that uses Google PubSub services.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_pubsub_async"
TOPIC_ID = f"topic-{DAG_ID}-{ENV_ID}"
MESSAGE = {
    "data": b"Tool",
    "attributes": {"name": "wrench", "mass": "1.3kg", "count": "3"},
}
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
    description="This Airflow DAG demonstrates asynchronous Pub/Sub message pulling using a sensor.",
    tags=["demo", "google_cloud", "pubsub"],
) as dag:
    create_topic = PubSubCreateTopicOperator(
        task_id="create_topic",
        topic=TOPIC_ID,
        project_id=PROJECT_ID,
        fail_if_exists=False,
    )
    subscribe_task = PubSubCreateSubscriptionOperator(
        task_id="subscribe_task", project_id=PROJECT_ID, topic=TOPIC_ID
    )
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[MESSAGE, MESSAGE],
    )
    subscription = subscribe_task.output
    pull_messages_async = PubSubPullSensor(
        task_id="pull_messages_async",
        ack_messages=True,
        project_id=PROJECT_ID,
        subscription=subscription,
        deferrable=True,
    )
    unsubscribe_task = PubSubDeleteSubscriptionOperator(
        task_id="unsubscribe_task", project_id=PROJECT_ID, subscription=subscription
    )
    unsubscribe_task.trigger_rule = TriggerRule.ALL_DONE
    delete_topic = PubSubDeleteTopicOperator(
        task_id="delete_topic", topic=TOPIC_ID, project_id=PROJECT_ID
    )
    delete_topic.trigger_rule = TriggerRule.ALL_DONE
    (
        create_topic
        >> subscribe_task
        >> publish_task
        >> pull_messages_async
        >> unsubscribe_task
        >> delete_topic
    )
