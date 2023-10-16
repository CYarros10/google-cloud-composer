"""
Reverse engineer the should-start SLA from Oozie.
"""

from datetime import datetime, timedelta, timezone
from airflow import models
from airflow.operators.python_operator import PythonOperator

#---------------------
# Universal DAG info
#---------------------
VERSION = "v0_0_1"

#-------------------------
# Tags, Default Args, and Macros
#-------------------------
tags = [
"application:samples"
]

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2023, 2,24),
    "sla": timedelta(minutes=20)
}
user_defined_macros = {
}

#-------------------------
# Callback Functions
#-------------------------
def should_start_sla(**context):
    """
    Replicate Oozie should-start SLA. context object contains templates_dict
    where should_start_sla value is set.

    <sla:should-start>${10 * MINUTES}</sla:should-start>
    """

    should_start_sla = context['templates_dict']['should_start_sla']
    time_list = should_start_sla.split(":")
    hours, minutes, seconds = map(int, time_list)
    should_start_sla_timedelta_obj = timedelta(hours=hours, minutes=minutes, seconds=seconds)

    expected_start_time = datetime.fromisoformat(context['templates_dict']['expected_start_time'])

    should_start_by = expected_start_time + should_start_sla_timedelta_obj

    return should_start_by > datetime.now(timezone.utc)


#-------------------------
# Begin DAG Generation
#-------------------------
with models.DAG(
    f"oozie_should_start_sla_{VERSION}",
    description="Reverse-engineer Oozie's should-start SLA.",
    schedule_interval="0 0 * * *", # daily
    tags=tags,
    default_args=default_args,
    user_defined_macros=user_defined_macros,
    is_paused_upon_creation=True,
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
) as dag:
    
    # # ----------------------------------
    # # start time sla check
    # # ----------------------------------

    should_start_sla_task = PythonOperator(
        task_id='should_start_sla_task', 
        python_callable=should_start_sla,
        provide_context=True,
        templates_dict={
            'should_start_sla': '{{ should_start_sla }}',
            'expected_start_time': '{{ ts }}'
        }
    )