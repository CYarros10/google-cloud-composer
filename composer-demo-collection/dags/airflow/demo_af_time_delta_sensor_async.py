"""
Example DAG demonstrating ``TimeDeltaSensorAsync``, a drop in replacement for ``TimeDeltaSensor`` that
defers and doesn't occupy a worker slot while it waits
"""

from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="demo_af_time_delta_sensor_async",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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
    description="This Airflow DAG demonstrates the TimeDeltaSensorAsync, a sensor that waits for a specified time without occupying a worker slot.",
    tags=["demo", "airflow", "sensors"],
) as dag:
    wait = TimeDeltaSensorAsync(task_id="wait", delta=timedelta(seconds=30))
    finish = EmptyOperator(task_id="finish")
    wait >> finish
