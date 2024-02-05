PROJECT_ID="cy-artifacts"
OLD_COMPOSER_ENV="composer-2-small"
NEW_COMPOSER_ENV="composer-2-small-new"
OLD_COMPOSER_LOCATION="us-central1"
NEW_COMPOSER_LOCATION="us-central1"
SNAPSHOT_GCS_FOLDER="gs://cy-sandbox/composer-snapshots/"

echo "... Begin user migration ..."

NEW_COMPOSER_BUCKET=$(gcloud composer environments describe ${NEW_COMPOSER_ENV} \
    --location ${NEW_COMPOSER_LOCATION} \
     --format="value(config.dagGcsPrefix)" | sed 's/\/dags$//')

echo "... Detailing users in old composer environment ..."

gcloud composer environments run \
    ${OLD_COMPOSER_ENV} \
    --location ${OLD_COMPOSER_LOCATION} \
    users list

echo "... Migrating users to new Composer environment ..."

gcloud composer environments run \
    ${OLD_COMPOSER_ENV} \
    --location ${OLD_COMPOSER_LOCATION} \
    users export -- /home/airflow/gcs/data/users.json

gcloud composer environments storage data export \
    --destination=${NEW_COMPOSER_BUCKET}/data \
    --environment=${OLD_COMPOSER_ENV} \
    --location=${OLD_COMPOSER_LOCATION} \
    --source=users.json

gcloud composer environments run \
    ${NEW_COMPOSER_ENV} \
    --location ${NEW_COMPOSER_LOCATION} \
    users import \
    -- /home/airflow/gcs/data/users.json

gcloud composer environments storage data delete \
    --environment=${OLD_COMPOSER_ENV} \
    --location=${OLD_COMPOSER_LOCATION}  \
    users.json

gcloud composer environments storage data delete \
    --environment=${NEW_COMPOSER_ENV} \
    --location=${NEW_COMPOSER_LOCATION}  \
    users.json

echo "... Detailing users in new composer environment ..."

gcloud composer environments run \
    ${NEW_COMPOSER_ENV} \
    --location ${NEW_COMPOSER_LOCATION} \
    users list