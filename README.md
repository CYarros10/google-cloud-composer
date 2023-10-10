# google-cloud-composer

A collection of Google Cloud Composer assets

## Guides

### Composer Guides

*   [What is Cloud Composer?](https://cloud.google.com/blog/topics/developers-practitioners/what-cloud-composer)
*   [Optimize Cloud Composer via Better Airflow DAGs](https://cloud.google.com/blog/products/data-analytics/optimize-cloud-composer-via-better-airflow-dags)
*   [Reduce Airflow DAG Parse Times in Cloud Composer](https://cloud.google.com/blog/products/data-analytics/reduce-airflow-dag-parse-times-in-cloud-composer)

### Airflow Guides

*   [Airflow Schedule Interval 101](https://towardsdatascience.com/airflow-schedule-interval-101-bbdda31cc463)
*

## Samples

Use these solutions as a reference for your own or extend them to fit your
particular use case.

### Airflow Samples
*   [Email Alerts](airflow-dag-samples/alerting/email_alert_dag.py) - A sample to send an email alert on DAG failure or SLA miss.
*   [Slack Alerts](airflow-dag-samples/alerting/slack_alert_dag.py) - A sample to send a slack alert on DAG failure or SLA miss.
*   [Config Driven DAGs](airflow-dag-samples/config-driven-dags) - A sample Airflow plugin to load a yaml config file into an Airflow DAG.
*   [BigQuery Hook](airflow-dag-samples/google-cloud-services/bq_hook_sample.py) - Sample of using the BigQuery hook in Airflw to get the number of
    rows affected by a BigQuery job.
*   [BigQuery Impersonation Chain](airflow-dag-samples/google-cloud-services/bq_impersonate_sa.py) - Sample of impersonating a service account for a
    BigQuery job.
*   [Cloud Function Hook](airflow-dag-samples/google-cloud-services/cf_hook_sample.py) - Sample of invoking a Cloud Function via the Cloud Functions hook
    and PythonOperator.
*   [Dataproc Operators](airflow-dag-samples/google-cloud-services/dataproc_cluster.py) - Samples of interacting with Dataproc clusters via Airflow.
*   [Dataproc Serverless Operators](airflow-dag-samples/google-cloud-services/dataproc_serverless.py) - Samples of interacting with Dataproc Serverless
    via Airflow.
*   [Dataproc Workflow Template Operators](airflow-dag-samples/google-cloud-services/dataproc_wft.py) - Samples of interacting with Dataproc workflow
    templates via Airflow.
*   [Resource Manager Operators](airflow-dag-samples/google-cloud-services/resource_manager_validation.py) - Sample for validating Google Organizational
    Folder and Project names.
*   [Dag Integrity Tests](airflow-unit-testing/test_dag_integrity.py) - A collection of unit tests for enforcing Airflow best practices.

### Composer Samples
*   [Composer CICD](composer-cicd/) - A sample CICD pipeline using Cloud Build to deploy dags to a Cloud Composer environment.
*   [Composer Terraform](composer-terraform/) - A collection of terraform modules for deploying Cloud Composer environments, cicd processes,
    and monitoring dashboards


## Tools

### Airflow DAG Development Tools

*   [Ruff](https://github.com/astral-sh/ruff) - An extremely fast Python linter, written in Rust. Great for linting Airflow DAGs.
*   [Black](https://pypi.org/project/black/) - Black is the uncompromising Python code formatter. 