# git clone https://github.com/GoogleCloudPlatform/java-docs-samples.git
# cd java-docs-samples
# git fetch origin pull/8442/head:bigtable-spark-connector-preview
# git checkout bigtable-spark-connector-preview
# cd bigtable/spark-connector-preview

BIGTABLE_SPARK_PROJECT_ID=cy-artifacts
BIGTABLE_SPARK_INSTANCE_ID=bt-test-instance
BIGTABLE_SPARK_TABLE_NAME=test
BIGTABLE_SPARK_DATAPROC_CLUSTER=pyspark-to-bt-test
BIGTABLE_SPARK_DATAPROC_REGION=us-central1
PATH_TO_PYTHON_FILE=/Users/yarros/code/spark_to_bigtable/python/word_count.py
PATH_TO_COMPILED_JAR=/Users/yarros/code/spark_to_bigtable/java-maven/target/bigtable-spark-example-0.0.1-SNAPSHOT.jar
BIGTABLE_SPARK_GCS_BUCKET_NAME=airflow-reporting-cy

gsutil cp $PATH_TO_COMPILED_JAR gs://airflow-reporting-cy/java/
gsutil cp $PATH_TO_PYTHON_FILE gs://airflow-reporting-cy/pyspark/word_count.py

gcloud dataproc clusters create \
$BIGTABLE_SPARK_DATAPROC_CLUSTER --region $BIGTABLE_SPARK_DATAPROC_REGION \
--master-machine-type n2-standard-4 --master-boot-disk-size 500 \
--num-workers 2 --worker-machine-type n2-standard-4 --worker-boot-disk-size 500 \
--image-version 2.0-debian10 --project $BIGTABLE_SPARK_PROJECT_ID

gcloud dataproc jobs submit spark \
    --cluster=$BIGTABLE_SPARK_DATAPROC_CLUSTER \
    --region=$BIGTABLE_SPARK_DATAPROC_REGION \
    --class=bigtable.spark.example.WordCount \
    --jars=$PATH_TO_COMPILED_JAR \
    --  \
    $BIGTABLE_SPARK_PROJECT_ID \
    $BIGTABLE_SPARK_INSTANCE_ID \
    $BIGTABLE_SPARK_TABLE_NAME

gcloud dataproc jobs submit pyspark \
    --cluster=$BIGTABLE_SPARK_DATAPROC_CLUSTER \
    --region=$BIGTABLE_SPARK_DATAPROC_REGION \
    --jars=gs://bigtable-spark-preview/jars/bigtable-spark-0.0.1-preview1-SNAPSHOT.jar \
    $PATH_TO_PYTHON_FILE \
    -- \
    --bigtableProjectId=$BIGTABLE_SPARK_PROJECT_ID \
    --bigtableInstanceId=$BIGTABLE_SPARK_INSTANCE_ID \
    --bigtableTableName=$BIGTABLE_SPARK_TABLE_NAME