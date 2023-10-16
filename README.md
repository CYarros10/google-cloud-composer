# google-cloud-composer

A collection of Google Cloud Composer assets

## Guides

### Composer Guides

*   [What is Cloud Composer?](https://cloud.google.com/blog/topics/developers-practitioners/what-cloud-composer)
*   [Optimize Cloud Composer via Better Airflow DAGs](https://cloud.google.com/blog/products/data-analytics/optimize-cloud-composer-via-better-airflow-dags)
*   [Reduce Airflow DAG Parse Times in Cloud Composer](https://cloud.google.com/blog/products/data-analytics/reduce-airflow-dag-parse-times-in-cloud-composer)

### Airflow Guides

*   [Airflow Schedule Interval 101](https://towardsdatascience.com/airflow-schedule-interval-101-bbdda31cc463)


## Samples

Use these solutions as a reference for your own or extend them to fit your
particular use case.

### Airflow Samples

*   [Bash DAG](airflow-dags/samples/bash_dag.py) - Sample DAG with a bash operator.
*   [BigQuery DAG](airflow-dags/samples/bigquery_dag.py) - Sample DAG with BigQuery operations.
*   [BigQuery Impersonation DAG](airflow-dags/samples/bigquery_dag.py) - Sample DAG with Service Account Impersonation.
*   [Cloud Storage DAG](airflow-dags/samples/cloudstorage_dag.py) - Sample DAG with Cloud Storage operations.
*   [Dataflow DAG](airflow-dags/samples/dataflow_dag.py) - Sample DAG with Dataflow operations.
*   [Dataproc DAG](airflow-dags/samples/dataproc_dag.py) - Sample DAG with Dataproc operations.
*   [Dataproc Serverless DAG](airflow-dags/samples/dataproc_serverless_dag.py) - Sample DAG with Dataproc Serverless operations.
*   [Dataproc Workflow Template DAG](airflow-dags/samples/dataproc_workflow_template_dag.py) - Sample DAG with Dataproc Workflow Template operations.
*   [PubSub Target DAG](airflow-dags/samples/pubsub_target_dag.py) - Sample of multi-dag dependency that acts as a target for a separate PubSub Trigger DAG.  
*   [PubSub Trigger Single DAG](airflow-dags/samples/pubsub_trigger_single_dag.py) - Sample of multi-dag dependency that acts as a trigger for a separate DAG.  
*   [Python DAG](airflow-dags/samples/python_dag.py) - Sample DAG with python operator.
*   [Resource Manager DAG](airflow-dags/samples/resource_manager_dag.py) - Sample DAG with Resource Manager operations.

### Composer Samples
*   [Composer CICD](composer-cicd/) - A sample CICD pipeline using Cloud Build to deploy dags to a Cloud Composer environment.
*   [Composer Terraform](composer-terraform/) - A collection of terraform modules for deploying Cloud Composer environments, cicd processes,
    and monitoring dashboards


## Tools

### Airflow DAG Development Tools

*   [Composer Local Development](https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments) - create, configure, and run a local Airflow environment using Composer Local Development CLI tool.
*   [Ruff](https://github.com/astral-sh/ruff) - An extremely fast Python linter, written in Rust. Great for linting Airflow DAGs.
*   [Black](https://pypi.org/project/black/) - Black is the uncompromising Python code formatter. 