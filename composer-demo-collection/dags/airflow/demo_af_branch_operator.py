"""Example DAG demonstrating the usage of the Classic branching Python operators.

It is showcasing the basic BranchPythonOperator and its sisters BranchExternalPythonOperator."""

import random
import sys
import tempfile
from pathlib import Path
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchExternalPythonOperator,
    BranchPythonOperator,
    ExternalPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

PATH_TO_PYTHON_BINARY = sys.executable
with DAG(
    dag_id="demo_af_branch_operator",
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
    tags=["demo", "airflow", "branching"],
    description="This Airflow DAG demonstrates the usage of the Classic branching Python operators.",
) as dag:
    run_this_first = EmptyOperator(task_id="run_this_first")
    options = ["a", "b", "c", "d"]
    branching = BranchPythonOperator(
        task_id="branching", python_callable=lambda: f"branch_{random.choice(options)}"
    )
    run_this_first >> branching
    join = EmptyOperator(
        task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    for option in options:
        t = PythonOperator(
            task_id=f"branch_{option}", python_callable=lambda: print("Hello World")
        )
        empty_follow = EmptyOperator(task_id="follow_" + option)
        branching >> Label(option) >> t >> empty_follow >> join

    def branch_with_external_python(choices):
        import random

        return f"ext_py_{random.choice(choices)}"

    branching_ext_py = BranchExternalPythonOperator(
        task_id="branching_ext_python",
        python=PATH_TO_PYTHON_BINARY,
        python_callable=branch_with_external_python,
        op_args=[options],
    )
    join >> branching_ext_py
    join_ext_py = EmptyOperator(
        task_id="join_ext_python", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    def hello_world_with_external_python():
        print("Hello World from external Python")

    for option in options:
        t = ExternalPythonOperator(
            task_id=f"ext_py_{option}",
            python=PATH_TO_PYTHON_BINARY,
            python_callable=hello_world_with_external_python,
        )
        branching_ext_py >> Label(option) >> t >> join_ext_py


    def hello_world_with_venv():
        import numpy as np

        print(f"Hello World with some numpy stuff: {np.arange(6)}")

    for option in options:
        t = PythonVirtualenvOperator(
            task_id=f"venv_{option}",
            requirements=["numpy~=1.24.4"],
            python_callable=hello_world_with_venv,
        )
        Label(option) >> t
