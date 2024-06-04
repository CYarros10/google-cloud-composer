# Cloud SQL Proxy Setup

## About

When you absolutely **need** to use Airflow to connect to Cloud SQL and return row results via XCOM or logs.

Otherwise: use [the native Google Cloud Operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/cloud_sql.html) such as:

- CloudSQLExecuteQueryOperator
- CloudSQLExportInstanceOperator

## Setup

**1) Update the YAML templates in the [config/](config/) directory with your resource information:**

- **composer environment GKE cluster:** (example) us-central1-composer-2-smal-56609903-gke
- **Cloud SQL Connection name:** (example) cy-artifacts:us-central1:test-gke-connect
- **Kubernetes Service Account:** (example) cloud-sql-ksa

**2) Update `setup.sh` varibales with your Google Cloud resource information**

**3) run `setup.sh`**

What does it do?

1. CREATE GOOGLE SERVICE ACCOUNT
2. CREATE IAM POLICY BINDING
3. AUTHENTICATE WITH COMPOSER GKE CLUSTER
4. CREATE KUBERNETES SERVICE ACCOUNT
5. CREATE KUBERNETES SECRET 
6. ENABLE WORKLOAD IDENTITY IN GKE CLUSTER
7. CREATE GSA-KSA BINDING
8. CREATE GKE ANNOTATION FOR KSA
9. APPLY SIDECAR DEPLOYMENT 
10. APPLY SIDECAR SERVICE

**4) Create an Airflow Connection to your Cloud SQL Proxy Service**

- **Connection ID:** cloud_sql_proxy_service
- **Connection Type:** MySQL
- **Host:** sql-proxy-deployment-service.default.svc.cluster.local
- **Schema:** [your mysql database]
- **Login:** [your mysql user]
- **Password:** [your mysql password]
- **Port:** 3306

**5) Deploy the sample [cloud_sql_proxy_dag.py](dags/cloud_sql_proxy_dag.py)** (it will probably fail if you haven't created the MySQL table yet)


## More Information

[Connecting-SQL-database-to-Airflow-in-Google-Cloud-Composer](https://www.googlecloudcommunity.com/gc/Databases/Connecting-SQL-database-to-Airflow-in-Google-Cloud-Composer/m-p/636090)

[Cloud SQL Proxy Docs](https://cloud.google.com/sql/docs/mysql/sql-proxy)

[Cloud SQL IAM Roles](https://cloud.google.com/sql/docs/mysql/iam-roles)

[Cloud SQL Proxy GitHub](https://github.com/GoogleCloudPlatform/cloud-sql-proxy/tree/main)

[Connecting Google Kubernetes Engine to Cloud SQL](https://cloud.google.com/sql/docs/postgres/connect-kubernetes-engine)
