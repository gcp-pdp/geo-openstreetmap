#!/bin/bash
DAG_NAME="$1"
COMPOSER_ENV_NAME="$2"
REGION_LOCATION="$3"

gcloud composer environments storage dags delete \
  --environment $COMPOSER_ENV_NAME  --location $REGION_LOCATION \
  $DAG_NAME
