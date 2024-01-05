PROJECT_ID="cy-artifacts"
OLD_COMPOSER_ENV="composer-2-small"
NEW_COMPOSER_ENV="composer-2-small-new"
OLD_COMPOSER_LOCATION="us-central1"
NEW_COMPOSER_LOCATION="us-central1"
SNAPSHOT_GCS_FOLDER="gs://cy-sandbox/composer-snapshots/"


echo "... Saving snapshot of old Composer environment ..."

SAVED_SNAPSHOT=$(gcloud beta composer environments snapshots save \
  ${OLD_COMPOSER_ENV} \
  --location ${OLD_COMPOSER_LOCATION} \
  --snapshot-location ${SNAPSHOT_GCS_FOLDER})

SAVED_SNAPSHOT_PATH=$(echo ${SAVED_SNAPSHOT} | awk '{split($0, a, ": "); print a[3]}')

echo "... Creating new Composer environment from snapshot ..."

gcloud beta composer environments snapshots load \
  ${NEW_COMPOSER_ENV} \
  --location ${NEW_COMPOSER_LOCATION} \
  --snapshot-path ${SAVED_SNAPSHOT_PATH}