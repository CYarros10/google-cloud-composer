# google-cloud-composer

A collection of Google Cloud Composer assets

---

## Guides

### Composer Guides

*   [What is Cloud Composer?](https://cloud.google.com/blog/topics/developers-practitioners/what-cloud-composer)
*   [Optimize Cloud Composer via Better Airflow DAGs](https://cloud.google.com/blog/products/data-analytics/optimize-cloud-composer-via-better-airflow-dags)
*   [Reduce Airflow DAG Parse Times in Cloud Composer](https://cloud.google.com/blog/products/data-analytics/reduce-airflow-dag-parse-times-in-cloud-composer)
*   [Cloud Composer Tenancy Strategies](https://cloud.google.com/blog/products/data-analytics/a-cloud-composer-tenancy-case-study)

### Airflow Guides

*   [Airflow Schedule Interval 101](https://towardsdatascience.com/airflow-schedule-interval-101-bbdda31cc463)
*   [Airflow Trigger Rules Guide](https://www.restack.io/docs/airflow-knowledge-trigger-rules-example)



---

## Sample Code

### Airflow Samples

#### Alerting

*   [Email Alerts](airflow-2-dags/alerting/email_alert_dag.py) - Sample of a DAG that sends an email on DAG failure or SLA miss. requires email setup beforehand.
*   [Slack Alerts](airflow-2-dags/alerting/slack_alert_dag.py) - Sample of a DAG that sends a message to a slack channel on DAG failure or SLA miss. requires slack channel setup beforehand.

#### Config Driven DAGs

*   [Local Configured DAG](airflow-2-dags/config-driven-dags/dags/local_configured_dag.py) - Sample of a DAG that loads a LOCAL YAML file for config variables.
*   [GCS Configured DAG](airflow-2-dags/config-driven-dags/dags/gcs_configured_dag.py) - Sample of a DAG that loads a GCS YAML file for config variables.

#### Monitoring and Reporting DAGs

*   [Advanced SLA Reporting DAG](airflow-2-dags/monitoring-and-reporting/advanced_sla_reporting_dag.py) - 
*   [Metadata Export DAG Factory](airflow-2-dags/monitoring-and-reporting/metadata_export_dag_factory.py) -

#### Multi-DAG Dependencies

*   [PubSub Deferred Single Trigger DAG](airflow-2-dags/multi-dag-dependencies/pubsub_deferred_trigger_single_dag.py) - 
*   [PubSub Target DAG](airflow-2-dags/multi-dag-dependencies/pubsub_target_dag.py) - Sample of multi-dag dependency that acts as a target for a separate PubSub Trigger DAG.  
*   [PubSub Trigger DAG List](airflow-2-dags/samples/pubsub_trigger_dag_list.py) - Sample of multi-dag dependency that acts as a trigger for a list of separate DAGs.  
*   [PubSub Trigger Single DAG](airflow-2-dags/samples/pubsub_trigger_single_dag.py) - Sample of multi-dag dependency that acts as a trigger for a separate DAG.  

#### Oozie Functionality in Airflow

*   [Should-start SLA](airflow-2-dags/oozie-functionality/should-start-sla.py) - Reverse engineer the should-start SLA from Oozie.

#### Operator Abstraction

*   [Abstract Cluster Create DAG](airflow-2-dags/operator-abstraction/abstract_cluster_create_dag.py) - 

#### Misc Samples

*   [Bash DAG](airflow-2-dags/samples/bash_dag.py) - Sample DAG with a bash operator.
*   [Basic Xcom DAG](airflow-2-dags/samples/basic_xcom_dag.py) - Sample DAG with basic xcom functionality.
*   [BigQuery DAG](airflow-2-dags/samples/bigquery_dag.py) - Sample DAG with BigQuery operations.
*   [BigQuery Impersonation DAG](airflow-2-dags/samples/bigquery_dag.py) - Sample DAG with Service Account Impersonation.
*   [Branching DAG](airflow-2-dags/samples/branch_operator_dag.py) - Sample DAG with basic branching functionality.
*   [Cloud SQL DAG](airflow-2-dags/samples/cloud_sql_dag.py) - Sample DAG with Cloud SQL operations.
*   [Cloud Storage DAG](airflow-2-dags/samples/cloudstorage_dag.py) - Sample DAG with Cloud Storage operations.
*   [Data Transfers DAG](airflow-2-dags/samples/data_transfers_dag.py) - Sample DAG with a variety of data transfer operators for Google Cloud.
*   [Dataflow DAG](airflow-2-dags/samples/dataflow_dag.py) - Sample DAG with Dataflow operations.
*   [Dataproc DAG](airflow-2-dags/samples/dataproc_dag.py) - Sample DAG with Dataproc operations.
*   [Dataproc Dynamic Xcom DAG](airflow-2-dags/samples/dataproc_dag.py) - Sample DAG with Dataproc operations and dynamic xcom functionality.
*   [Dataproc Serverless DAG](airflow-2-dags/samples/dataproc_serverless_dag.py) - Sample DAG with Dataproc Serverless operations.
*   [Dataproc Workflow Template DAG](airflow-2-dags/samples/dataproc_workflow_template_dag.py) - Sample DAG with Dataproc Workflow Template operations.
*   [Flink DAG](airflow-2-dags/samples/flink_dag.py) - Various ways to deploy a Flink job on dataproc using Airflow.
*   [Params DAG](airflow-2-dags/samples/params_dag.py) - Sample DAG with basic parameter usage.
*   [Python DAG](airflow-2-dags/samples/python_dag.py) - Sample DAG with python operator.
*   [Resource Manager DAG](airflow-2-dags/samples/resource_manager_dag.py) - Sample DAG with Resource Manager operations.

#### Spark to Bigtable

*   [Spark to Bigtable DAG](airflow-2-dags/spark-to-bigtable/spark_to_bigtable_dag.py) - Sample of a DAG that reads/writes to Bigtable using Spark and PySpark.

### Airflow REST API

*   [Samples](airflow-rest-api/samples.py) - A sample python script interacting with the Airflow REST API for basic operations.

### Composer Samples

*   [Composer CICD](composer-cicd/) - A sample CICD pipeline using Cloud Build to deploy dags to a Cloud Composer environment.
*   [Composer Terraform](composer-terraform/) - A collection of terraform modules for deploying Cloud Composer environments, cicd processes,
    and monitoring dashboards

---

## Tools

### Airflow DAG Development Tools

*   [Composer Local Development](https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments) - create, configure, and run a local Airflow environment using Composer Local Development CLI tool.
*   [Ruff](https://github.com/astral-sh/ruff) - An extremely fast Python linter, written in Rust. Great for linting Airflow DAGs.
*   [Black](https://pypi.org/project/black/) - Black is the uncompromising Python code formatter. 

---

## More Examples

*   [Airflow DAGs](https://github.com/apache/airflow/tree/main/airflow/example_dags) - General purpose, standard Airflow DAGs.
*   [Google Cloud Airflow DAGs](https://github.com/apache/airflow/tree/main/airflow/providers/google/cloud/example_dags) - Various Google Cloud DAGs.
*   [Many More Google Cloud Airflow DAGs](https://github.com/apache/airflow/tree/main/tests/system/providers/google/cloud) - Various Google Cloud DAGs.



