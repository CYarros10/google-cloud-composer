"""
Example Airflow DAG that uses Google PubSub services.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_pubsub"
TOPIC_ID = f"topic-{DAG_ID}-{ENV_ID}"
MESSAGE = {
    "data": b"Tool",
    "attributes": {"name": "wrench", "mass": "1.3kg", "count": "3"},
}
MESSAGE_TWO = {
    "data": b"Tool",
    "attributes": {"name": "wrench", "mass": "1.2kg", "count": "2"},
}
echo_cmd = "\n{% for m in task_instance.xcom_pull('pull_messages') %}\n    echo \"AckID: {{ m.get('ackId') }}, Base64-Encoded: {{ m.get('message') }}\"\n{% endfor %}\n"
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
    description="This Airflow DAG demonstrates Google PubSub services, including creating topics, subscriptions, publishing messages, pulling messages, and deleting topics and subscriptions. \n",
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
    subscription = subscribe_task.output
    pull_messages = PubSubPullSensor(
        task_id="pull_messages",
        ack_messages=True,
        project_id=PROJECT_ID,
        subscription=subscription,
    )
    pull_messages_result = BashOperator(
        task_id="pull_messages_result", bash_command=echo_cmd
    )
    pull_messages_operator = PubSubPullOperator(
        task_id="pull_messages_operator",
        ack_messages=True,
        project_id=PROJECT_ID,
        subscription=subscription,
    )
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[MESSAGE, MESSAGE],
    )
    publish_task2 = PubSubPublishMessageOperator(
        task_id="publish_task2",
        project_id=PROJECT_ID,
        topic=TOPIC_ID,
        messages=[MESSAGE_TWO, MESSAGE_TWO],
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
        >> pull_messages
        >> pull_messages_result
        >> publish_task2
        >> pull_messages_operator
        >> unsubscribe_task
        >> delete_topic
    )
