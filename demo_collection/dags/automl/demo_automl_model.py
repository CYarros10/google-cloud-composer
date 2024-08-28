"""
Example Airflow DAG for Google AutoML service testing model operations.
"""

from copy import deepcopy
from datetime import datetime, timedelta
from google.protobuf.struct_pb2 import Value
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLBatchPredictOperator,
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLDeleteModelOperator,
    AutoMLDeployModelOperator,
    AutoMLGetModelOperator,
    AutoMLImportDataOperator,
    AutoMLPredictOperator,
    AutoMLTablesListColumnSpecsOperator,
    AutoMLTablesListTableSpecsOperator,
    AutoMLTablesUpdateDatasetOperator,
    AutoMLTrainModelOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_automl_model"
LOCATION_REGION = "us-central1"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATASET_NAME = f"dataset_{DAG_ID}".replace("-", "_")
DATASET = {
    "display_name": DATASET_NAME,
    "tables_dataset_metadata": {"target_column_spec_id": ""},
}
AUTOML_DATASET_BUCKET = (
    f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl/bank-marketing-split.csv"
)
IMPORT_INPUT_CONFIG = {"gcs_source": {"input_uris": [AUTOML_DATASET_BUCKET]}}
IMPORT_OUTPUT_CONFIG = {
    "gcs_destination": {
        "output_uri_prefix": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/automl"
    }
}
MODEL_NAME = f"md_tabular_{ENV_ID}".replace("-", "_")
MODEL = {
    "display_name": MODEL_NAME,
    "tables_model_metadata": {"train_budget_milli_node_hours": 1000},
}
PREDICT_VALUES = [
    Value(string_value="TRAINING"),
    Value(string_value="51"),
    Value(string_value="blue-collar"),
    Value(string_value="married"),
    Value(string_value="primary"),
    Value(string_value="no"),
    Value(string_value="620"),
    Value(string_value="yes"),
    Value(string_value="yes"),
    Value(string_value="cellular"),
    Value(string_value="29"),
    Value(string_value="jul"),
    Value(string_value="88"),
    Value(string_value="10"),
    Value(string_value="-1"),
    Value(string_value="0"),
    Value(string_value="unknown"),
]
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
        "target": "Deposit",
        "extract_object_id": extract_object_id,
    },
    description="Airflow DAG for Google Cloud AutoML model testing: creates a bucket, imports data, trains a model, predicts, and cleans up.",
    tags=["demo", "google_cloud", "automl"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=LOCATION_REGION,
    )
    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_data_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="automl/datasets/model",
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
    MODEL["dataset_id"] = dataset_id
    import_dataset = AutoMLImportDataOperator(
        task_id="import_dataset",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        input_config=IMPORT_INPUT_CONFIG,
        project_id=PROJECT_ID
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
        table_spec_id="{{ extract_object_id(task_instance.xcom_pull('list_tables_spec')[0]) }}",
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    update = deepcopy(DATASET)
    update["name"] = '{{ task_instance.xcom_pull("create_dataset")["name"] }}'
    update["tables_dataset_metadata"][
        "target_column_spec_id"
    ] = "{{ get_target_column_spec(task_instance.xcom_pull('list_columns_spec'), target) }}"
    update_dataset = AutoMLTablesUpdateDatasetOperator(
        task_id="update_dataset", dataset=update, location=LOCATION_REGION
    )
    create_model = AutoMLTrainModelOperator(
        task_id="create_model",
        model=MODEL,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    model_id = create_model.output["model_id"]
    get_model = AutoMLGetModelOperator(
        task_id="get_model",
        model_id=model_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    deploy_model = AutoMLDeployModelOperator(
        task_id="deploy_model",
        model_id=model_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    predict_task = AutoMLPredictOperator(
        task_id="predict_task",
        model_id=model_id,
        payload={"row": {"values": PREDICT_VALUES}},
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    batch_predict_task = AutoMLBatchPredictOperator(
        task_id="batch_predict_task",
        model_id=model_id,
        input_config=IMPORT_INPUT_CONFIG,
        output_config=IMPORT_OUTPUT_CONFIG,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_model = AutoMLDeleteModelOperator(
        task_id="delete_model",
        model_id=model_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
    )
    delete_dataset = AutoMLDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=dataset_id,
        location=LOCATION_REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
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
        >> create_model
        >> get_model
        >> deploy_model
        >> predict_task
        >> batch_predict_task
        >> delete_model
        >> delete_dataset
        >> delete_bucket
    )
