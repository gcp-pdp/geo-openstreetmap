#!/bin/bash
OSM_ENV_VARS_STR="$1"
COMPOSER_ENV_NAME="$2"
REGION_LOCATION="$3"

gcloud composer environments update $COMPOSER_ENV_NAME \
  --location $REGION_LOCATION \
  --update-env-variables=$OSM_ENV_VARS_STR