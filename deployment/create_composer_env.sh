#!/bin/bash
COMPOSER_ENV_NAME="$1"
REGION_LOCATION="$2"

gcloud composer environments create $COMPOSER_ENV_NAME \
    --location $REGION_LOCATION