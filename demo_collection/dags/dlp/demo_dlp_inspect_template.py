"""
Example Airflow DAG that creates, updates, list and deletes Data Loss Prevention inspect templates.
"""

from datetime import datetime, timedelta
from google.cloud.dlp_v2.types import ContentItem, InspectConfig, InspectTemplate
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateInspectTemplateOperator,
    CloudDLPDeleteInspectTemplateOperator,
    CloudDLPGetInspectTemplateOperator,
    CloudDLPInspectContentOperator,
    CloudDLPListInspectTemplatesOperator,
    CloudDLPUpdateInspectTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dlp_inspect_template"
ENV_ID = "composer"
PROJECT_ID = "your-project"
TEMPLATE_ID = f"dlp-inspect-{ENV_ID}"
ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(
    info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}]
)
INSPECT_TEMPLATE = InspectTemplate(inspect_config=INSPECT_CONFIG)
with DAG(
    DAG_ID,
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
        "sla": timedelta(minutes=55),
    },
    description="This Airflow DAG creates, updates, lists, and deletes Data Loss Prevention inspect templates.",
    tags=["demo", "google_cloud", "dlp"],
) as dag:
    create_template = CloudDLPCreateInspectTemplateOperator(
        task_id="create_template",
        project_id=PROJECT_ID,
        inspect_template=INSPECT_TEMPLATE,
        template_id=TEMPLATE_ID,
        do_xcom_push=True,
    )
    list_templates = CloudDLPListInspectTemplatesOperator(
        task_id="list_templates", project_id=PROJECT_ID
    )
    get_template = CloudDLPGetInspectTemplateOperator(
        task_id="get_template", project_id=PROJECT_ID, template_id=TEMPLATE_ID
    )
    update_template = CloudDLPUpdateInspectTemplateOperator(
        task_id="update_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        inspect_template=INSPECT_TEMPLATE,
    )
    inspect_content = CloudDLPInspectContentOperator(
        task_id="inspect_content",
        project_id=PROJECT_ID,
        item=ITEM,
        inspect_template_name="{{ task_instance.xcom_pull('create_template', key='return_value')['name'] }}",
    )
    delete_template = CloudDLPDeleteInspectTemplateOperator(
        task_id="delete_template", template_id=TEMPLATE_ID, project_id=PROJECT_ID
    )
    delete_template.trigger_rule = TriggerRule.ALL_DONE
    (
        create_template
        >> list_templates
        >> get_template
        >> update_template
        >> inspect_content
        >> delete_template
    )
