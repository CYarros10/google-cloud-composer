"""
Apache Airflow DAG for example email alerts:
- sla misses
- dag failures
"""

import time
import random
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.email_operator import EmailOperator
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

def email_on_failure(receiver, subject, context):

    html_content = f"""
    DAG Run failed with following details:
    <br> <b>dag_id</b>: {context.get('task_instance').dag_id}
    <br> <b>task_id</b>: {context.get('task_instance').task_id}
    <br> <b>logical date</b>: {context.get('logical_date')}
    <br> <b>start_time</b>: {context.get('task_instance').start_date}
    <br> <b>end_time</b>: {context.get('task_instance').end_date}
    <br> <b>log_url</b>: {context.get('task_instance').log_url}
    <br> <b>error_msg</b>: {context.get('exception')}
    """

    email_alert = EmailOperator(
        task_id='email_alert',
        to=receiver,
        subject=subject,
        html_content=html_content,
        mime_charset='utf-8'
    )
    return email_alert.execute(context)

def email_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):

    receiver="sla_miss_receiver@test.com"
    subject = f"DAG {dag.dag_id} SLA Miss"

    html_content = f"""
    DAG Run missed SLA:
    <br> <b>DAG</b>: {dag.dag_id}
    <br> <b>DAG Description</b>: {dag.description}
    <br> <b>Dag Tags</b>: {dag.tags}
    <br> <b>Task List</b>: {task_list}
    <br> <b>Blocking</b>: {blocking_task_list}
    <br> <b>slas</b>: {slas}
    <br> <b>blocking task ids</b>: {blocking_tis}
    """
    
    email_alert = EmailOperator(
        task_id='email_alert',
        to=receiver,
        subject=subject,
        html_content=html_content,
        mime_charset='utf-8'
    )
    
    return email_alert.execute()

#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"email_alert_demo_{VERSION}",
    schedule_interval="0 0 * * *", # daily
    default_args=default_args,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=email_on_failure,
    sla_miss_callback=email_on_sla_miss
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