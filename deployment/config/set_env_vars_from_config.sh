#!/bin/bash
CONFIG_FILE="$1"
COMPOSER_ENV_NAME="$2"
REGION_LOCATION="$3"

declare -A VARS_ARRAY
while IFS="=" read -r key value
do
    VARS_ARRAY[$key]="$value"
done < <(jq -r "to_entries|map(\"\(.key)=\(.value|tostring)\")|.[]"  $CONFIG_FILE)

OSM_ENV_VARS_STR=''
for key in "${!VARS_ARRAY[@]}"
do
    OSM_ENV_VARS_STR="${OSM_ENV_VARS_STR}${key^^}=${VARS_ARRAY[$key]},"
done

OSM_ENV_VARS_STR=${OSM_ENV_VARS_STR::-1}
echo $OSM_ENV_VARS_STR

gcloud composer environments update $COMPOSER_ENV_NAME \
  --location $REGION_LOCATION \
  --update-env-variables=$OSM_ENV_VARS_STR