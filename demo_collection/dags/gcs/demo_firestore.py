"""
Example Airflow DAG showing export of database from Google Cloud Firestore to Cloud Storage.

This example creates collections in the default namespaces in Firestore, adds some data to the collection
and exports this database from Cloud Firestore to Cloud Storage.
It then creates a table from the exported data in Bigquery and checks that the data is in it.
"""

from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreCommitOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.firebase.operators.firestore import (
    CloudFirestoreExportDatabaseOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = "composer"
PROJECT_ID = "your-project"
DAG_ID = "demo_firestore_to_gcp"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")
EXPORT_DESTINATION_URL = f"gs://{BUCKET_NAME}/namespace"
EXPORT_COLLECTION_ID = f"collection_{DAG_ID}_{ENV_ID}".replace("-", "_")
EXTERNAL_TABLE_SOURCE_URI = f"{EXPORT_DESTINATION_URL}/all_namespaces/kind_{EXPORT_COLLECTION_ID}/all_namespaces_kind_{EXPORT_COLLECTION_ID}.export_metadata"
DATASET_LOCATION = "EU"
KEYS = {
    "partitionId": {"projectId": PROJECT_ID, "namespaceId": ""},
    "path": {"kind": f"{EXPORT_COLLECTION_ID}"},
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
    description="This Airflow DAG exports a database from Cloud Firestore to BigQuery, verifies the data, and then deletes the intermediate resources.",
    tags=["demo", "google_cloud", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, location=DATASET_LOCATION
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        location=DATASET_LOCATION,
        project_id=PROJECT_ID,
    )
    commit_task = CloudDatastoreCommitOperator(
        task_id="commit_task",
        body={
            "mode": "TRANSACTIONAL",
            "mutations": [
                {
                    "insert": {
                        "key": KEYS,
                        "properties": {"string": {"stringValue": "test!"}},
                    }
                }
            ],
            "singleUseTransaction": {"readWrite": {}},
        },
        project_id=PROJECT_ID,
    )
    export_database_to_gcs = CloudFirestoreExportDatabaseOperator(
        task_id="export_database_to_gcs",
        project_id=PROJECT_ID,
        body={
            "outputUriPrefix": EXPORT_DESTINATION_URL,
            "collectionIds": [EXPORT_COLLECTION_ID],
        },
    )
    create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_NAME,
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": "firestore_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "DATASTORE_BACKUP",
                "compression": "NONE",
                "sourceUris": [EXTERNAL_TABLE_SOURCE_URI],
            },
        },
    )
    read_data_from_gcs_multiple_types = BigQueryInsertJobOperator(
        task_id="execute_query",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.firestore_data`",
                "useLegacySql": False,
            }
        },
    )
    delete_entity = DataflowTemplatedJobStartOperator(
        task_id="delete-entity-firestore",
        project_id=PROJECT_ID,
        template="gs://dataflow-templates/latest/Firestore_to_Firestore_Delete",
        parameters={
            "firestoreReadGqlQuery": f"SELECT * FROM {EXPORT_COLLECTION_ID}",
            "firestoreReadProjectId": PROJECT_ID,
            "firestoreDeleteProjectId": PROJECT_ID,
        },
        environment={"tempLocation": f"gs://{BUCKET_NAME}/tmp"},
        location="us-central1",
        append_job_name=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        project_id=PROJECT_ID,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    end = EmptyOperator(task_id="end")
    (
        [create_bucket, create_dataset]
        >> commit_task
        >> export_database_to_gcs
        >> create_external_table_multiple_types
        >> read_data_from_gcs_multiple_types
        >> delete_entity
        >> [delete_dataset, delete_bucket]
    )
    read_data_from_gcs_multiple_types >> end
