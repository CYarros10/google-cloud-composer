"""
Apache Airflow DAG for example log alerts:
- sla misses
- dag failures
"""

import time
import random
import logging
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_5"
ON_DAG_FAILURE_ALERT = "Airflow DAG Failure:"
ON_SLA_MISS_ALERT = "Airflow DAG SLA Miss:"
ON_TASK_FAILURE_ALERT = "Airflow Task Failure:"

#-------------------------
# Tags, Default Args, and Macros
#-------------------------


default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 3, 1),
    "sla": timedelta(minutes=1)
}

#-------------------------
# Callback Functions
#-------------------------

def log_on_dag_failure(context):
    """
    collect DAG information and send to console.log on failure.
    """
    dag = context.get('dag')

    log_msg = f"""
    {ON_DAG_FAILURE_ALERT}
    *DAG*: {dag.dag_id}
    *DAG Description*: {dag.description}
    *DAG Tags*: {dag.tags}
    *Context*: {context}
    """

    logging.info(log_msg)

def log_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    collect DAG information and send to console.log on sla miss.
    """

    log_msg = f"""
        {ON_SLA_MISS_ALERT}
        *DAG*: {dag.dag_id}
        *DAG Description*: {dag.description}
        *DAG Tags*: {dag.tags}
        *Task List*: {task_list}
        *Blocking*: {blocking_task_list}
        *slas*: {slas}
        *blocking task ids*: {blocking_tis}
    """

    logging.info(log_msg)


def log_on_task_failure(context):
    ti = context.get('task_instance')
    log_msg = f"""
        {ON_TASK_FAILURE_ALERT}
        *DAG*: {ti.dag_id}
        *DAG Run*: {ti.run_id}
        *Task*: {ti.task_id}
        *state*: {ti.state}
        *operator*: {ti.operator}
    """

    logging.info(log_msg)


#-------------------------
# Begin DAG Generation
#-------------------------
for i in range(4,50):
    with models.DAG(
        f"log_alert_demo_{VERSION}_{i}",
        schedule_interval=f"*/{i} * * * *", # every i minutes
        default_args=default_args,
        is_paused_upon_creation=True,
        catchup=False,
        max_active_runs=5,
        dagrun_timeout=timedelta(minutes=30),
        on_failure_callback=log_on_dag_failure,
        sla_miss_callback=log_on_sla_miss,
    ) as dag:

        def three_minute_run():
            """
            run for three minutes
            """
            time.sleep(180)

            # 30% chance to raise an exception
            if random.randint(0,9) % 3 == 0:
                raise AirflowException("Error msg")

        def one_minute_run():
            """
            run for one minutes
            """
            time.sleep(60)

            # 50% chance to raise an exception
            if random.randint(0,8) % 2 == 0:
                raise AirflowException("Error msg")

        three_minute_task = PythonOperator(
            task_id='three_minute_task', 
            python_callable=three_minute_run,
        )

        one_minute_task_1 = PythonOperator(
            task_id='one_minute_task_1', 
            python_callable=one_minute_run,
            on_failure_callback=log_on_task_failure
        )

        one_minute_task_2 = PythonOperator(
            task_id='one_minute_task_2', 
            python_callable=one_minute_run,
            on_failure_callback=log_on_task_failure
        )

        three_minute_task >> one_minute_task_1 >> one_minute_task_2
