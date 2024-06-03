"""
Example Airflow DAG that shows the complex DAG structure.
"""

import pendulum
from datetime import timedelta
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo_af_complex",
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
    description="This DAG demonstrates various Data Catalog operations, including create, delete, get, list, lookup, rename, search, and update.",
    tags=["demo", "airflow"],
) as dag:
    create_entry_group = BashOperator(
        task_id="create_entry_group", bash_command="echo create_entry_group"
    )
    create_entry_group_result = BashOperator(
        task_id="create_entry_group_result",
        bash_command="echo create_entry_group_result",
    )
    create_entry_group_result2 = BashOperator(
        task_id="create_entry_group_result2",
        bash_command="echo create_entry_group_result2",
    )
    create_entry_gcs = BashOperator(
        task_id="create_entry_gcs", bash_command="echo create_entry_gcs"
    )
    create_entry_gcs_result = BashOperator(
        task_id="create_entry_gcs_result", bash_command="echo create_entry_gcs_result"
    )
    create_entry_gcs_result2 = BashOperator(
        task_id="create_entry_gcs_result2", bash_command="echo create_entry_gcs_result2"
    )
    create_tag = BashOperator(task_id="create_tag", bash_command="echo create_tag")
    create_tag_result = BashOperator(
        task_id="create_tag_result", bash_command="echo create_tag_result"
    )
    create_tag_result2 = BashOperator(
        task_id="create_tag_result2", bash_command="echo create_tag_result2"
    )
    create_tag_template = BashOperator(
        task_id="create_tag_template", bash_command="echo create_tag_template"
    )
    create_tag_template_result = BashOperator(
        task_id="create_tag_template_result",
        bash_command="echo create_tag_template_result",
    )
    create_tag_template_result2 = BashOperator(
        task_id="create_tag_template_result2",
        bash_command="echo create_tag_template_result2",
    )
    create_tag_template_field = BashOperator(
        task_id="create_tag_template_field",
        bash_command="echo create_tag_template_field",
    )
    create_tag_template_field_result = BashOperator(
        task_id="create_tag_template_field_result",
        bash_command="echo create_tag_template_field_result",
    )
    create_tag_template_field_result2 = BashOperator(
        task_id="create_tag_template_field_result2",
        bash_command="echo create_tag_template_field_result",
    )
    delete_entry = BashOperator(
        task_id="delete_entry", bash_command="echo delete_entry"
    )
    create_entry_gcs >> delete_entry
    delete_entry_group = BashOperator(
        task_id="delete_entry_group", bash_command="echo delete_entry_group"
    )
    create_entry_group >> delete_entry_group
    delete_tag = BashOperator(task_id="delete_tag", bash_command="echo delete_tag")
    create_tag >> delete_tag
    delete_tag_template_field = BashOperator(
        task_id="delete_tag_template_field",
        bash_command="echo delete_tag_template_field",
    )
    delete_tag_template = BashOperator(
        task_id="delete_tag_template", bash_command="echo delete_tag_template"
    )
    get_entry_group = BashOperator(
        task_id="get_entry_group", bash_command="echo get_entry_group"
    )
    get_entry_group_result = BashOperator(
        task_id="get_entry_group_result", bash_command="echo get_entry_group_result"
    )
    get_entry = BashOperator(task_id="get_entry", bash_command="echo get_entry")
    get_entry_result = BashOperator(
        task_id="get_entry_result", bash_command="echo get_entry_result"
    )
    get_tag_template = BashOperator(
        task_id="get_tag_template", bash_command="echo get_tag_template"
    )
    get_tag_template_result = BashOperator(
        task_id="get_tag_template_result", bash_command="echo get_tag_template_result"
    )
    list_tags = BashOperator(task_id="list_tags", bash_command="echo list_tags")
    list_tags_result = BashOperator(
        task_id="list_tags_result", bash_command="echo list_tags_result"
    )
    lookup_entry = BashOperator(
        task_id="lookup_entry", bash_command="echo lookup_entry"
    )
    lookup_entry_result = BashOperator(
        task_id="lookup_entry_result", bash_command="echo lookup_entry_result"
    )
    rename_tag_template_field = BashOperator(
        task_id="rename_tag_template_field",
        bash_command="echo rename_tag_template_field",
    )
    search_catalog = BashOperator(
        task_id="search_catalog", bash_command="echo search_catalog"
    )
    search_catalog_result = BashOperator(
        task_id="search_catalog_result", bash_command="echo search_catalog_result"
    )
    update_entry = BashOperator(
        task_id="update_entry", bash_command="echo update_entry"
    )
    update_tag = BashOperator(task_id="update_tag", bash_command="echo update_tag")
    update_tag_template = BashOperator(
        task_id="update_tag_template", bash_command="echo update_tag_template"
    )
    update_tag_template_field = BashOperator(
        task_id="update_tag_template_field",
        bash_command="echo update_tag_template_field",
    )
    create_tasks = [
        create_entry_group,
        create_entry_gcs,
        create_tag_template,
        create_tag_template_field,
        create_tag,
    ]
    chain(*create_tasks)
    create_entry_group >> delete_entry_group
    create_entry_group >> create_entry_group_result
    create_entry_group >> create_entry_group_result2
    create_entry_gcs >> delete_entry
    create_entry_gcs >> create_entry_gcs_result
    create_entry_gcs >> create_entry_gcs_result2
    create_tag_template >> delete_tag_template_field
    create_tag_template >> create_tag_template_result
    create_tag_template >> create_tag_template_result2
    create_tag_template_field >> delete_tag_template_field
    create_tag_template_field >> create_tag_template_field_result
    create_tag_template_field >> create_tag_template_field_result2
    create_tag >> delete_tag
    create_tag >> create_tag_result
    create_tag >> create_tag_result2
    delete_tasks = [
        delete_tag,
        delete_tag_template_field,
        delete_tag_template,
        delete_entry_group,
        delete_entry,
    ]
    chain(*delete_tasks)
    create_tag_template >> get_tag_template >> delete_tag_template
    get_tag_template >> get_tag_template_result
    create_entry_gcs >> get_entry >> delete_entry
    get_entry >> get_entry_result
    create_entry_group >> get_entry_group >> delete_entry_group
    get_entry_group >> get_entry_group_result
    create_tag >> list_tags >> delete_tag
    list_tags >> list_tags_result
    create_entry_gcs >> lookup_entry >> delete_entry
    lookup_entry >> lookup_entry_result
    create_tag_template_field >> rename_tag_template_field >> delete_tag_template_field
    chain(create_tasks, search_catalog, delete_tasks)
    search_catalog >> search_catalog_result
    create_entry_gcs >> update_entry >> delete_entry
    create_tag >> update_tag >> delete_tag
    create_tag_template >> update_tag_template >> delete_tag_template
    create_tag_template_field >> update_tag_template_field >> rename_tag_template_field
