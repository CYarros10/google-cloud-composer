"""Example DAG demonstrating the usage DAG params to model a trigger UI with a user form.

This example DAG generates greetings to a list of provided names in selected languages in the logs.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
with DAG(
    dag_id="demo_af_params_trigger_ui",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs. Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
    description="This Airflow DAG generates greetings in selected languages for a list of names provided in the UI.",
    tags=["demo", "airflow", "params", "triggers"],
) as dag:

    @task(task_id="get_names")
    def get_names(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        if "names" not in dag_run.conf:
            print("Uuups, no names given, was no UI used to trigger?")
            return []
        return dag_run.conf["names"]

    @task.branch(task_id="select_languages")
    def select_languages(**kwargs) -> list[str]:
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        selected_languages = []
        for lang in ["english", "german", "french"]:
            if lang in dag_run.conf and dag_run.conf[lang]:
                selected_languages.append(f"generate_{lang}_greeting")
        return selected_languages

    @task(task_id="generate_english_greeting")
    def generate_english_greeting(name: str) -> str:
        return f"Hello {name}!"

    @task(task_id="generate_german_greeting")
    def generate_german_greeting(name: str) -> str:
        return f"Sehr geehrter Herr/Frau {name}."

    @task(task_id="generate_french_greeting")
    def generate_french_greeting(name: str) -> str:
        return f"Bonjour {name}!"

    @task(task_id="print_greetings", trigger_rule=TriggerRule.ALL_DONE)
    def print_greetings(greetings1, greetings2, greetings3) -> None:
        for g in greetings1 or []:
            print(g)
        for g in greetings2 or []:
            print(g)
        for g in greetings3 or []:
            print(g)
        if not (greetings1 or greetings2 or greetings3):
            print("sad, nobody to greet :-(")

    lang_select = select_languages()
    names = get_names()
    english_greetings = generate_english_greeting.expand(name=names)
    german_greetings = generate_german_greeting.expand(name=names)
    french_greetings = generate_french_greeting.expand(name=names)
    lang_select >> [english_greetings, german_greetings, french_greetings]
    results_print = print_greetings(
        english_greetings, german_greetings, french_greetings
    )
