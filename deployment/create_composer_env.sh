#!/bin/bash
COMPOSER_ENV_NAME="$1"
REGION_LOCATION="$2"
DISK_SIZE="$3"
MACHINE_TYPE="$4"

gcloud composer environments create $COMPOSER_ENV_NAME \
    --location $REGION_LOCATION \
    --disk-size $DISK_SIZE \
    --machine-type $MACHINE_TYPE
