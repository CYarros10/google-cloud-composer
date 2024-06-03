"""
Example Airflow DAG for Google AutoML service testing dataset operations.
"""

from copy import deepcopy
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLImportDataOperator,
    AutoMLListDatasetOperator,
    AutoMLTablesListColumnSpecsOperator,
    AutoMLTablesListTableSpecsOperator,
    AutoMLTablesUpdateDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "demo_automl_dataset"
ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
LOCATION_REGION = "us-central1"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
DATASET_NAME = f"dataset_{DAG_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "tables_dataset_metadata": {"target_column_spec_id": ""},
}
AUTOML_DATASET_BUCKET = (
    f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/tabular-classification.csv"
)
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}
extract_object_id = CloudAutoMLHook.extract_object_id


def get_target_column_spec(columns_specs: list[dict], column_name: str) -> str:
    """
    Using column name returns spec of the column.
    """
    for column in columns_specs:
        if column["display_name"] == column_name:
            return extract_object_id(column)
    raise Exception(f"Unknown target column: {column_name}")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=360),
    max_active_runs=1,
    default_args={
        "owner": "Google",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(minutes=330),
    },
    user_defined_macros={
        "get_target_column_spec": get_target_column_spec,
        "target": "Class",
        "extract_object_id": extract_object_id,
    },
    description="This Airflow DAG creates and manipulates an AutoML dataset, including creating a GCS bucket, importing data, and deleting the dataset. \n",
    tags=["demo", "google_cloud", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="automl/datasets/tabular",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="automl",
        recursive=True,
    )
    create_dataset = AutoMLCreateDatasetOperator(
        task_id="create_dataset",
        dataset=DATASET,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    dataset_id = create_dataset.output["dataset_id"]
    import_dataset = AutoMLImportDataOperator(
        task_id="import_dataset",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        input_config=IMPORT_INPUT_CONFIG,
        project_id=PROJECT_ID,
    )
    list_tables_spec = AutoMLTablesListTableSpecsOperator(
        task_id="list_tables_spec",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    list_columns_spec = AutoMLTablesListColumnSpecsOperator(
        task_id="list_columns_spec",
        dataset_id=dataset_id,
        table_spec_id="{{ extract_object_id(task_instance.xcom_pull('list_tables_spec_task')[0]) }}",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    update = deepcopy(DATASET)
    update["name"] = '{{ task_instance.xcom_pull("create_dataset")["name"] }}'
    update["tables_dataset_metadata"][
        "target_column_spec_id"
    ] = "{{ get_target_column_spec(task_instance.xcom_pull('list_columns_spec_task'), target) }}"
    update_dataset = AutoMLTablesUpdateDatasetOperator(
        task_id="update_dataset", dataset=update, location=LOCATION_REGION
    )
    list_datasets = AutoMLListDatasetOperator(
        task_id="list_datasets", location=LOCATION_REGION, project_id=PROJECT_ID
    )
    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (
        [create_bucket >> move_dataset_file, create_dataset]
        >> import_dataset
        >> list_tables_spec
        >> list_columns_spec
        >> update_dataset
        >> list_datasets
        >> delete_dataset
        >> delete_bucket
    )
