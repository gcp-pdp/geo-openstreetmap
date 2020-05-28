#!/bin/bash
DAG_ELEMENT_PATH="$1"
COMPOSER_ENV_NAME="$2"
REGION_LOCATION="$3"

echo "Uploading $DAG_ELEMENT_PATH ..."
gcloud composer environments storage dags import \
    --environment $COMPOSER_ENV_NAME --location $REGION_LOCATION \
    --source $DAG_ELEMENT_PATH
