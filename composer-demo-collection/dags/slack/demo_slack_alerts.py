"""
Apache Airflow DAG for example slack alerts:
- sla misses
- dag failures
"""

import time
import random
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow import AirflowException
from slack_gemini import message_on_failure


for i in range(3, 7):
    with models.DAG(
        f"demo_slack_alerts_{i}",
        schedule_interval=f"*/{i} * * * *",
        is_paused_upon_creation=True,
        catchup=False,
        max_active_runs=5,
        dagrun_timeout=timedelta(minutes=5),
        on_failure_callback=message_on_failure,
        description="This Airflow DAG demonstrates Slack alert setup for task failures and SLA misses, with tasks simulating time-consuming processes with varying failure probabilities.",
        tags=["demo", "slack"],
    ) as dag:

        def three_minute_run():
            """
            run for three minutes
            """
            time.sleep(180)
            if random.randint(0, 9) % 3 == 0: # 30% chance error
                raise AirflowException("Error msg")

        def one_minute_run():
            """
            run for one minutes
            """
            time.sleep(60)
            if random.randint(0, 8) % 2 == 0: # 50% chance error
                raise AirflowException("Error msg")

        three_minute_task = PythonOperator(
            task_id="three_minute_task", python_callable=three_minute_run
        )
        one_minute_task_1 = PythonOperator(
            task_id="one_minute_task_1",
            python_callable=one_minute_run,
        )
        one_minute_task_2 = PythonOperator(
            task_id="one_minute_task_2",
            python_callable=one_minute_run,
        )
        three_minute_task >> one_minute_task_1 >> one_minute_task_2
