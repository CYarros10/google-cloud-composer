from __future__ import annotations
from datetime import datetime, timedelta, timezone
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.sensors.time_sensor import TimeSensor, TimeSensorAsync
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weekday import WeekDay


def success_callable():
    return True


def failure_callable():
    return False


with DAG(
    dag_id="demo_af_sensors",
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
    description="This Airflow DAG demonstrates various sensor operators, including time, bash, file, python, and day of week sensors.",
    tags=["demo", "airflow", "sensors"],
) as dag:
    t0 = TimeDeltaSensor(
        task_id="wait_some_seconds", delta=timedelta(seconds=10)
    )
    t0a = TimeDeltaSensorAsync(
        task_id="wait_some_seconds_async", delta=timedelta(seconds=10)
    )
    t1 = TimeSensor(
        task_id="fire_immediately",
        target_time=datetime.now(tz=timezone.utc).time(),
    )
    t2 = TimeSensor(
        task_id="timeout_after_second_date_in_the_future",
        timeout=1,
        soft_fail=True,
        target_time=(
            datetime.now(tz=timezone.utc)
            + timedelta(minutes=5)
        ).time(),
    )
    t1a = TimeSensorAsync(
        task_id="fire_immediately_async",
        target_time=datetime.now(tz=timezone.utc).time(),
    )
    t2a = TimeSensorAsync(
        task_id="timeout_after_second_date_in_the_future_async",
        timeout=10,
        soft_fail=True,
        target_time=(
            datetime.now(tz=timezone.utc)
            + timedelta(hours=1)
        ).time(),
    )
    t3 = BashSensor(task_id="Sensor_succeeds", bash_command="exit 0")
    t4 = BashSensor(
        task_id="Sensor_fails_after_10_seconds",
        timeout=10,
        soft_fail=True,
        bash_command="exit 1",
    )
    t5 = BashOperator(
        task_id="remove_file", bash_command="rm -rf /tmp/temporary_file_for_testing"
    )
    t6 = FileSensor(task_id="wait_for_file", filepath="/tmp/temporary_file_for_testing")
    t7 = FileSensor(
        task_id="wait_for_file_async",
        filepath="/tmp/temporary_file_for_testing",
        #deferrable=True,
    )
    t8 = BashOperator(
        task_id="create_file_after_3_seconds",
        bash_command="sleep 3; touch /tmp/temporary_file_for_testing",
    )
    t9 = PythonSensor(task_id="success_sensor_python", python_callable=success_callable)
    t10 = PythonSensor(
        task_id="failure_timeout_sensor_python",
        timeout=10,
        soft_fail=True,
        python_callable=failure_callable,
    )
    t11 = DayOfWeekSensor(
        task_id="week_day_sensor_failing_on_timeout",
        timeout=10,
        soft_fail=True,
        week_day=WeekDay.MONDAY,
    )
    tx = BashOperator(task_id="print_date_in_bash", bash_command="date")
    tx.trigger_rule = TriggerRule.NONE_FAILED
    [t0, t0a, t1, t1a, t2, t2a, t3, t4] >> tx
    t5 >> t6 >> t7 >> tx
    t8 >> tx
    [t9, t10] >> tx
    t11 >> tx
