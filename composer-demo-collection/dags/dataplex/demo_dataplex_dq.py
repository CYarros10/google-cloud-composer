"""
Example Airflow DAG that shows how to use Dataplex Scan Data.
"""

from datetime import datetime, timedelta
from google.cloud import dataplex_v1
from google.cloud.dataplex_v1 import DataQualitySpec
from google.protobuf.field_mask_pb2 import FieldMask
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateAssetOperator,
    DataplexCreateLakeOperator,
    DataplexCreateOrUpdateDataQualityScanOperator,
    DataplexCreateZoneOperator,
    DataplexDeleteAssetOperator,
    DataplexDeleteDataQualityScanOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteZoneOperator,
    DataplexGetDataQualityScanOperator,
    DataplexGetDataQualityScanResultOperator,
    DataplexRunDataQualityScanOperator,
)
from airflow.providers.google.cloud.sensors.dataplex import (
    DataplexDataQualityJobStatusSensor,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "cy-artifacts"
DAG_ID = "demo_dataplex_data_quality"
LAKE_ID = f"test-lake-{ENV_ID}"
LOCATION_REGION = "us-central1"
DATASET_NAME = f"dataset_bq_{ENV_ID}"
TABLE_1 = "table0"
TABLE_2 = "table1"
SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "dt", "type": "STRING", "mode": "NULLABLE"},
]
DATASET = DATASET_NAME
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE_1} VALUES (1, 'test test2', '{INSERT_DATE}');"
)
LOCATION = "us"
TRIGGER_SPEC_TYPE = "ON_DEMAND"
ZONE_ID = "test-zone-id"
DATA_SCAN_ID = "test-data-scan-id"
EXAMPLE_LAKE_BODY = {
    "display_name": "test_display_name",
    "labels": [],
    "description": "test_description",
    "metastore": {"service": ""},
}
EXAMPLE_ZONE = {"type_": "RAW", "resource_spec": {"location_type": "SINGLE_REGION"}}
ASSET_ID = "test-asset-id"
EXAMPLE_ASSET = {
    "resource_spec": {
        "name": f"projects/{PROJECT_ID}/datasets/{DATASET_NAME}",
        "type_": "BIGQUERY_DATASET",
    },
    "discovery_spec": {"enabled": True},
}
EXAMPLE_DATA_SCAN = dataplex_v1.DataScan()
EXAMPLE_DATA_SCAN.data.entity = f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/lakes/{LAKE_ID}/zones/{ZONE_ID}/entities/{TABLE_1}"
EXAMPLE_DATA_SCAN.data.resource = f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET}/tables/{TABLE_1}"
EXAMPLE_DATA_SCAN.data_quality_spec = DataQualitySpec(
    {
        "rules": [
            {
                "range_expectation": {"min_value": "0", "max_value": "10000"},
                "column": "value",
                "dimension": "VALIDITY",
            }
        ]
    }
)
UPDATE_MASK = FieldMask(paths=["data_quality_spec"])
ENTITY = f"projects/{PROJECT_ID}/locations/{LOCATION_REGION}/lakes/{LAKE_ID}/zones/{ZONE_ID}/entities/{TABLE_1}"
EXAMPLE_DATA_SCAN_UPDATE = {
    "data": {
        "entity": ENTITY,
        "resource": f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET}/tables/{TABLE_1}",
    },
    "data_quality_spec": {
        "rules": [
            {
                "range_expectation": {"min_value": "1", "max_value": "50000"},
                "column": "value",
                "dimension": "VALIDITY",
            }
        ]
    },
}
with DAG(
    DAG_ID,
    start_date=datetime(2024, 1, 1),
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
    description="This DAG demonstrates how to use Dataplex Scan Data to run data quality checks on a BigQuery table. The DAG will create a BigQuery dataset and table, insert data into the table and then run a data quality scan against the table. Finally, the DAG will clean up the resources it created.\n",
    tags=["demo", "google_cloud", "dataplex", "bigquery", "data_quality"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )
    create_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET_NAME,
        table_id=TABLE_1,
        schema_fields=SCHEMA,
        location=LOCATION,
    )
    create_table_2 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_2",
        dataset_id=DATASET_NAME,
        table_id=TABLE_2,
        schema_fields=SCHEMA,
        location=LOCATION,
    )
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={"query": {"query": INSERT_ROWS_QUERY, "useLegacySql": False}},
    )
    create_lake = DataplexCreateLakeOperator(
        task_id="create_lake",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        body=EXAMPLE_LAKE_BODY,
        lake_id=LAKE_ID,
    )
    create_zone = DataplexCreateZoneOperator(
        task_id="create_zone",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_ZONE,
        zone_id=ZONE_ID,
    )
    create_asset = DataplexCreateAssetOperator(
        task_id="create_asset",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        body=EXAMPLE_ASSET,
        lake_id=LAKE_ID,
        zone_id=ZONE_ID,
        asset_id=ASSET_ID,
    )
    create_data_scan = DataplexCreateOrUpdateDataQualityScanOperator(
        task_id="create_data_scan",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        body=EXAMPLE_DATA_SCAN,
        data_scan_id=DATA_SCAN_ID,
    )
    update_data_scan = DataplexCreateOrUpdateDataQualityScanOperator(
        task_id="update_data_scan",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        update_mask=UPDATE_MASK,
        body=EXAMPLE_DATA_SCAN_UPDATE,
        data_scan_id=DATA_SCAN_ID,
    )
    get_data_scan = DataplexGetDataQualityScanOperator(
        task_id="get_data_scan",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    run_data_scan_sync = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_sync",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    get_data_scan_job_result = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    run_data_scan_async = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_async",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
        asynchronous=True,
    )
    get_data_scan_job_status = DataplexDataQualityJobStatusSensor(
        task_id="get_data_scan_job_status",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
        job_id="{{ task_instance.xcom_pull('run_data_scan_async') }}",
    )
    get_data_scan_job_result_2 = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result_2",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    run_data_scan_def = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_def",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
        deferrable=True,
    )
    run_data_scan_async_2 = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_async_2",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
        asynchronous=True,
    )
    get_data_scan_job_result_def = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result_def",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
        deferrable=True,
    )
    delete_asset = DataplexDeleteAssetOperator(
        task_id="delete_asset",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        zone_id=ZONE_ID,
        asset_id=ASSET_ID,
    )
    delete_asset.trigger_rule = TriggerRule.ALL_DONE
    delete_zone = DataplexDeleteZoneOperator(
        task_id="delete_zone",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        zone_id=ZONE_ID,
    )
    delete_zone.trigger_rule = TriggerRule.ALL_DONE
    delete_data_scan = DataplexDeleteDataQualityScanOperator(
        task_id="delete_data_scan",
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        data_scan_id=DATA_SCAN_ID,
    )
    delete_data_scan.trigger_rule = TriggerRule.ALL_DONE
    delete_lake = DataplexDeleteLakeOperator(
        project_id=PROJECT_ID,
        region=LOCATION_REGION,
        lake_id=LAKE_ID,
        task_id="delete_lake",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        project_id=PROJECT_ID,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    chain(
        create_dataset,
        [create_table_1, create_table_2],
        insert_query_job,
        create_lake,
        create_zone,
        create_asset,
        create_data_scan,
        update_data_scan,
        get_data_scan,
        run_data_scan_sync,
        get_data_scan_job_result,
        run_data_scan_async,
        get_data_scan_job_status,
        get_data_scan_job_result_2,
        run_data_scan_def,
        run_data_scan_async_2,
        get_data_scan_job_result_def,
        delete_asset,
        delete_zone,
        delete_data_scan,
        [delete_lake, delete_dataset],
    )
