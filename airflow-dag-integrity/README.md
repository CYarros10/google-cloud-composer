# DAG Integrity

## Linting

Use [ruff](https://github.com/astral-sh/ruff), an extremely fast Python linter, written in Rust.

## Formatting

Use [black](https://pypi.org/project/black/), an uncompromising Python code formatter.

## Integrity Tests

Follow [Google Cloud Blog - Optimize Cloud Composer via Better Airflow DAGs](https://cloud.google.com/blog/products/data-analytics/optimize-cloud-composer-via-better-airflow-dags) - a guide containing a generalized checklist of activities when authoring Apache Airflow DAGs. 

test_dags.py enforces the following best practices from the guide above and provides many deprecation warnings:

* No Import Errors
* Valid Schedule Interval
* Owner Present
* SLA Present
* SLA Less Than Timeout
* Retries Present and value = 1-4
* Catchup Set to False by Default
* DAG Timeout set
* DAG Description set
* DAG Paused on Create
* DAG Valid Tags
* DAG Check Task Cycle
* DAG Import Time Less Than 2 Seconds
