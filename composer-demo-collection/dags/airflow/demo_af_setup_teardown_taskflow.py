"""Example DAG demonstrating the usage of setup and teardown tasks."""

from datetime import timedelta
import pendulum
from airflow.decorators import setup, task, task_group, teardown
from airflow.models.dag import DAG

with DAG(
    dag_id="demo_af_setup_teardown_taskflow",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",
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
    description="This Airflow DAG demonstrates the usage of setup and teardown tasks.",
    tags=["demo", "airflow", "teardown"],
) as dag:

    @task
    def my_first_task():
        print("Hello 1")

    @task
    def my_second_task():
        print("Hello 2")

    @task
    def my_third_task():
        print("Hello 3")

    task_1 = my_first_task()
    task_2 = my_second_task()
    task_3 = my_third_task()
    task_1 >> task_2 >> task_3.as_teardown(setups=task_1)

    @setup
    def outer_setup():
        print("I am outer_setup")
        return "some cluster id"

    @teardown
    def outer_teardown(cluster_id):
        print("I am outer_teardown")
        print(f"Tearing down cluster: {cluster_id}")

    @task
    def outer_work():
        print("I am just a normal task")

    @task_group
    def section_1():

        @setup
        def inner_setup():
            print("I set up")
            return "some_cluster_id"

        @task
        def inner_work(cluster_id):
            print(f"doing some work with cluster_id={cluster_id!r}")

        @teardown
        def inner_teardown(cluster_id):
            print(f"tearing down cluster_id={cluster_id!r}")

        inner_setup_task = inner_setup()
        inner_work(inner_setup_task) >> inner_teardown(inner_setup_task)

    with outer_teardown(outer_setup()):
        outer_work()
        section_1()
