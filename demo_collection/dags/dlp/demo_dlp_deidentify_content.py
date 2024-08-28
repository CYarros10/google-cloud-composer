"""
Example Airflow DAG that de-identifies potentially sensitive info using Data Loss Prevention operators.
"""

from datetime import datetime, timedelta
from google.cloud.dlp_v2.types import ContentItem, DeidentifyTemplate, InspectConfig
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dlp import (
    CloudDLPCreateDeidentifyTemplateOperator,
    CloudDLPDeidentifyContentOperator,
    CloudDLPDeleteDeidentifyTemplateOperator,
    CloudDLPGetDeidentifyTemplateOperator,
    CloudDLPListDeidentifyTemplatesOperator,
    CloudDLPReidentifyContentOperator,
    CloudDLPUpdateDeidentifyTemplateOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_dlp_deidentify_content"
ENV_ID = "composer"
PROJECT_ID = "your-project"
CRYPTO_KEY_NAME = f"{DAG_ID}_{ENV_ID}"
ITEM = ContentItem(
    table={
        "headers": [{"name": "column1"}],
        "rows": [{"values": [{"string_value": "My phone number is (206) 555-0123"}]}],
    }
)
INSPECT_CONFIG = InspectConfig(
    info_types=[{"name": "PHONE_NUMBER"}, {"name": "US_TOLLFREE_PHONE_NUMBER"}]
)
DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "replace_config": {
                        "new_value": {"string_value": "[deidentified_number]"}
                    }
                }
            }
        ]
    }
}
REVERSIBLE_DEIDENTIFY_CONFIG = {
    "info_type_transformations": {
        "transformations": [
            {
                "primitive_transformation": {
                    "crypto_deterministic_config": {
                        "crypto_key": {"transient": {"name": CRYPTO_KEY_NAME}},
                        "surrogate_info_type": {"name": "PHONE_NUMBER"},
                    }
                }
            }
        ]
    }
}
TEMPLATE_ID = f"template_{DAG_ID}_{ENV_ID}"
DEIDENTIFY_TEMPLATE = DeidentifyTemplate(deidentify_config=DEIDENTIFY_CONFIG)
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
    description="This Airflow DAG de-identifies potentially sensitive information using DLP operators.",
    tags=["demo", "google_cloud", "dlp"],
) as dag:
    deidentify_content = CloudDLPDeidentifyContentOperator(
        project_id=PROJECT_ID,
        item=ITEM,
        deidentify_config=DEIDENTIFY_CONFIG,
        inspect_config=INSPECT_CONFIG,
        task_id="deidentify_content",
    )
    reidentify_content = CloudDLPReidentifyContentOperator(
        task_id="reidentify_content",
        project_id=PROJECT_ID,
        item=ContentItem(
            value="{{ task_instance.xcom_pull('deidentify_content')['item'] }}"
        ),
        reidentify_config=REVERSIBLE_DEIDENTIFY_CONFIG,
        inspect_config=INSPECT_CONFIG,
    )
    create_template = CloudDLPCreateDeidentifyTemplateOperator(
        task_id="create_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        deidentify_template=DEIDENTIFY_TEMPLATE,
    )
    list_templates = CloudDLPListDeidentifyTemplatesOperator(
        task_id="list_templates", project_id=PROJECT_ID
    )
    get_template = CloudDLPGetDeidentifyTemplateOperator(
        task_id="get_template", project_id=PROJECT_ID, template_id=TEMPLATE_ID
    )
    update_template = CloudDLPUpdateDeidentifyTemplateOperator(
        task_id="update_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        deidentify_template=DEIDENTIFY_TEMPLATE,
    )
    deidentify_content_with_template = CloudDLPDeidentifyContentOperator(
        project_id=PROJECT_ID,
        item=ITEM,
        deidentify_template_name="{{ task_instance.xcom_pull('create_template')['name'] }}",
        inspect_config=INSPECT_CONFIG,
        task_id="deidentify_content_with_template",
    )
    delete_template = CloudDLPDeleteDeidentifyTemplateOperator(
        task_id="delete_template",
        project_id=PROJECT_ID,
        template_id=TEMPLATE_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        deidentify_content
        >> reidentify_content
        >> create_template
        >> list_templates
        >> get_template
        >> update_template
        >> deidentify_content_with_template
        >> delete_template
    )
