"""
Example DAG demonstrating the usage of the classic Python operators to execute Python functions natively and
within a virtual environment.
"""

from datetime import timedelta
import logging
import sys
import time
from pprint import pprint
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)

log = logging.getLogger(__name__)
PATH_TO_PYTHON_BINARY = sys.executable
with DAG(
    dag_id="demo_af_python_operator",
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
    description="This Airflow DAG demonstrates the usage of the classic Python operators to execute Python functions natively and within a virtual environment.",
    tags=["demo", "airflow", "python"],
):

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
        task_id="print_the_context", python_callable=print_context
    )

    def log_sql(**kwargs):
        logging.info(
            "Python task decorator query: %s", str(kwargs["templates_dict"]["query"])
        )

    log_the_sql = PythonOperator(
        task_id="log_sql_query",
        python_callable=log_sql,
        templates_dict={"query": "sql/sample.sql"},
        templates_exts=[".sql"],
    )

    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    for i in range(5):
        sleeping_task = PythonOperator(
            task_id=f"sleep_for_{i}",
            python_callable=my_sleeping_function,
            op_kwargs={"random_base": i / 10},
        )
        run_this >> log_the_sql >> sleeping_task
    if not is_venv_installed():
        log.warning(
            "The virtalenv_python example task requires virtualenv, please install it."
        )
    else:

        def callable_virtualenv():
            """
            Example function that will be performed in a virtual environment.

            Importing at the module level ensures that it will not attempt to import the
            library before it is installed.
            """
            from time import sleep
            from colorama import Back, Fore, Style

            print(Fore.RED + "some red text")
            print(Back.GREEN + "and with a green background")
            print(Style.DIM + "and in dim text")
            print(Style.RESET_ALL)
            for _ in range(4):
                print(Style.DIM + "Please wait...", flush=True)
                sleep(1)
            print("Finished")

        virtualenv_task = PythonVirtualenvOperator(
            task_id="virtualenv_python",
            python_callable=callable_virtualenv,
            requirements=["colorama==0.4.0"],
            system_site_packages=False,
        )
        sleeping_task >> virtualenv_task

        def callable_external_python():
            """
            Example function that will be performed in a virtual environment.

            Importing at the module level ensures that it will not attempt to import the
            library before it is installed.
            """
            import sys
            from time import sleep

            print(f"Running task via {sys.executable}")
            print("Sleeping")
            for _ in range(4):
                print("Please wait...", flush=True)
                sleep(1)
            print("Finished")

        external_python_task = ExternalPythonOperator(
            task_id="external_python",
            python_callable=callable_external_python,
            python=PATH_TO_PYTHON_BINARY,
        )
        run_this >> external_python_task >> virtualenv_task
