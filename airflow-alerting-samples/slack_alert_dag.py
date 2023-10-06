"""
Apache Airflow DAG for example slack alerts:
- sla misses
- dag failures
"""

import time
import random
from datetime import datetime, timedelta
from airflow import models
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_1"

#-------------------------
# Tags, Default Args, and Macros
#-------------------------


default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 2,24),
    "sla": timedelta(minutes=1)
}

#-------------------------
# Callback Functions
#-------------------------

# Send slack notification from our failure callback
def slack_on_failure(context):
    
    dag = context.get('dag')

    slack_msg = f"""
    :rotating_light: DAG Failed :rotating_light:
    *DAG*: {dag.dag_id}
    *DAG Description*: {dag.description}
    *DAG Tags*: {dag.tags}
    *Context*: {context}
    """

    alert = SlackWebhookOperator(
        task_id='slack_sla_notification',
        slack_webhook_conn_id='slack_webhook',
        message=slack_msg,
        icon_emoji=":robot_face:")
    
    return alert.execute(slack_msg)

def slack_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):

    message = f"""
        :warning: DAG SLA Missed :warning:
        *DAG*: {dag.dag_id}
        *DAG Description*: {dag.description}
        *DAG Tags*: {dag.tags}
        *Task List*: {task_list}
        *Blocking*: {blocking_task_list}
        *slas*: {slas}
        *blocking task ids*: {blocking_tis}
    """
    
    alert = SlackWebhookOperator(
        task_id='slack_sla_notification',
        slack_webhook_conn_id='slack_webhook',
        message=message)
    
    return alert.execute(message)

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"slack_alert_demo_{VERSION}",
    schedule_interval="0 0 * * *", # daily
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=slack_on_failure,
    sla_miss_callback=slack_on_sla_miss
) as dag:

    def ten_minute_run():
        """
        run for ten minutes
        """
        time.sleep(600)

        # 30% chance to raise an exception
        if random.randint(0,9) % 3 == 0:
            raise AirflowException("Error msg")
    
    ten_minute_task = PythonOperator(
        task_id='ten_minute_task', 
        python_callable=ten_minute_run,
    )