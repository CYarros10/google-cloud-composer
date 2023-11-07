# google-cloud-composer

A collection of Google Cloud Composer assets

## Guides

### Composer Guides

*   [What is Cloud Composer?](https://cloud.google.com/blog/topics/developers-practitioners/what-cloud-composer)
*   [Optimize Cloud Composer via Better Airflow DAGs](https://cloud.google.com/blog/products/data-analytics/optimize-cloud-composer-via-better-airflow-dags)
*   [Reduce Airflow DAG Parse Times in Cloud Composer](https://cloud.google.com/blog/products/data-analytics/reduce-airflow-dag-parse-times-in-cloud-composer)
*   [Cloud Composer Tenancy Strategies](https://cloud.google.com/blog/products/data-analytics/a-cloud-composer-tenancy-case-study)


### Airflow Guides

*   [Airflow Schedule Interval 101](https://towardsdatascience.com/airflow-schedule-interval-101-bbdda31cc463)

## Sample Code

### Airflow Samples

#### Alerting

*   [Email Alerts](airflow-dags/alerting/email_alert_dag.py) - Sample of a DAG that sends an email on DAG failure or SLA miss. requires email setup beforehand.
*   [Slack Alerts](airflow-dags/alerting/slack_alert_dag.py) - Sample of a DAG that sends a message to a slack channel on DAG failure or SLA miss. requires slack channel setup beforehand.

#### Config Driven DAGs

*   [Local Configured DAG](airflow-dags/config-driven-dags/dags/local_configured_dag.py) - Sample of a DAG that loads a LOCAL YAML file for config variables.
*   [GCS Configured DAG](airflow-dags/config-driven-dags/dags/gcs_configured_dag.py) - Sample of a DAG that loads a GCS YAML file for config variables.

#### Misc. DAGs

*   [Bash DAG](airflow-dags/samples/bash_dag.py) - Sample DAG with a bash operator.
*   [Basic Xcom DAG](airflow-dags/samples/basic_xcom_dag.py) - Sample DAG with basic xcom functionality.
*   [BigQuery DAG](airflow-dags/samples/bigquery_dag.py) - Sample DAG with BigQuery operations.
*   [BigQuery Impersonation DAG](airflow-dags/samples/bigquery_dag.py) - Sample DAG with Service Account Impersonation.
*   [Cloud Storage DAG](airflow-dags/samples/cloudstorage_dag.py) - Sample DAG with Cloud Storage operations.
*   [Data Transfers DAG](airflow-dags/samples/data_transfers_dag.py) - Sample DAG with a variety of data transfer operators for Google Cloud.
*   [Dataflow DAG](airflow-dags/samples/dataflow_dag.py) - Sample DAG with Dataflow operations.
*   [Dataproc DAG](airflow-dags/samples/dataproc_dag.py) - Sample DAG with Dataproc operations.
*   [Dataproc Dynamic Xcom DAG](airflow-dags/samples/dataproc_dag.py) - Sample DAG with Dataproc operations and dynamic xcom functionality.
*   [Dataproc Serverless DAG](airflow-dags/samples/dataproc_serverless_dag.py) - Sample DAG with Dataproc Serverless operations.
*   [Dataproc Workflow Template DAG](airflow-dags/samples/dataproc_workflow_template_dag.py) - Sample DAG with Dataproc Workflow Template operations.
*   [Params DAG](airflow-dags/samples/params_dag.py) - Sample DAG with basic parameter usage.
*   [Python DAG](airflow-dags/samples/python_dag.py) - Sample DAG with python operator.
*   [Resource Manager DAG](airflow-dags/samples/resource_manager_dag.py) - Sample DAG with Resource Manager operations.

#### Multi-DAG Dependencies

*   [PubSub Target DAG](airflow-dags/multi-dag-dependencies/pubsub_target_dag.py) - Sample of multi-dag dependency that acts as a target for a separate PubSub Trigger DAG.  
*   [PubSub Trigger DAG List](airflow-dags/samples/pubsub_trigger_dag_list.py) - Sample of multi-dag dependency that acts as a trigger for a list of separate DAGs.  
*   [PubSub Trigger Single DAG](airflow-dags/samples/pubsub_trigger_single_dag.py) - Sample of multi-dag dependency that acts as a trigger for a separate DAG.  


#### Oozie Functionality in Airflow

*   [Should-start SLA](airflow-dags/oozie-functionality/should-start-sla.py) - Reverse engineer the should-start SLA from Oozie.

#### Spark to Bigtable

*   [Spark to Bigtable DAG](airflow-dags/spark-to-bigtable/spark_to_bigtable_dag.py) - Sample of a DAG that reads/writes to Bigtable using Spark and PySpark.

### Airflow REST API

*   [Samples](airflow-rest-api/samples.py) - A sample python script interacting with the Airflow REST API for basic operations.

### Composer Samples

*   [Composer CICD](composer-cicd/) - A sample CICD pipeline using Cloud Build to deploy dags to a Cloud Composer environment.
*   [Composer Terraform](composer-terraform/) - A collection of terraform modules for deploying Cloud Composer environments, cicd processes,
    and monitoring dashboards

## Tools

### Airflow DAG Development Tools

*   [Composer Local Development](https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments) - create, configure, and run a local Airflow environment using Composer Local Development CLI tool.
*   [Ruff](https://github.com/astral-sh/ruff) - An extremely fast Python linter, written in Rust. Great for linting Airflow DAGs.
*   [Black](https://pypi.org/project/black/) - Black is the uncompromising Python code formatter. 
