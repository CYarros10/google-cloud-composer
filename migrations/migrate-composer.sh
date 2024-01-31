#!/bin/bash

# The script takes three arguments
# 1. (p) project_id where composer is deployed
# 2. (c) old composer environment name
# 3. (l) old composer environment location
# 1. (C) new composer environment name
# 2. (L) new composer environment location 
# 3. (g) gcs location for snapshot

# The script estimates the min workers and max workers based on the metrics. 
# Make sure it is valid/aplies for you installation (by deploying in lower environments))
# The script downloads the the airflow.cfg. Copies the relevant information to new environment
usage()
{
    echo "usage: migrate-composer.sh options:<p|c|l|C|L|g>"
}
PROJECT_ID=""
OLD_COMPOSER_ENV=""
OLD_COMPOSER_LOCATION=""
NEW_COMPOSER_ENV=""
NEW_COMPOSER_LOCATION=""
SNAPSHOT_GCS_FOLDER=""
while getopts p:c:l:C:L:g: flag
do
    case "${flag}" in
        p) PROJECT_ID=${OPTARG};;
        c) OLD_COMPOSER_ENV=${OPTARG};;
        l) OLD_COMPOSER_LOCATION=${OPTARG};;
        C) NEW_COMPOSER_ENV=${OPTARG};;
        L) NEW_COMPOSER_LOCATION=${OPTARG};;
        g) SNAPSHOT_GCS_FOLDER=${OPTARG};;
        *) usage
           exit;;
    esac
done

[[ $PROJECT_ID == "" || $OLD_COMPOSER_ENV == "" || $OLD_COMPOSER_LOCATION == "" || $NEW_COMPOSER_ENV = "" || $NEW_COMPOSER_LOCATION = "" || $SNAPSHOT_GCS_FOLDER = "" ]] && { usage; exit 1; }

# PROJECT_ID="cy-artifacts"
# OLD_COMPOSER_ENV="composer-2-small"
# OLD_COMPOSER_LOCATION="us-central1"

# NEW_COMPOSER_ENV="composer-2-small-new"
# NEW_COMPOSER_LOCATION="us-central1"
# SNAPSHOT_GCS_FOLDER="gs://cy-sandbox/composer-snapshots/"


echo "... Saving snapshot of old Composer environment ..."

SAVED_SNAPSHOT=$(gcloud beta composer environments snapshots save \
  ${OLD_COMPOSER_ENV} \
  --location ${OLD_COMPOSER_LOCATION} \
  --snapshot-location ${SNAPSHOT_GCS_FOLDER})

echo "... Pausing all DAGs in old Composer environment ..."

gcloud composer environments run ${OLD_COMPOSER_ENV} \
  --location ${OLD_COMPOSER_LOCATION} \
  dags trigger -- "pause_all_dags_dag_v1"

SAVED_SNAPSHOT_PATH=$(echo ${SAVED_SNAPSHOT} | awk '{split($0, a, ": "); print a[3]}')

echo "... Loading snapshot into new Composer environment ..."

gcloud beta composer environments snapshots load \
  ${NEW_COMPOSER_ENV} \
  --location ${NEW_COMPOSER_LOCATION} \
  --snapshot-path ${SAVED_SNAPSHOT_PATH}

echo "COMPLETE!"