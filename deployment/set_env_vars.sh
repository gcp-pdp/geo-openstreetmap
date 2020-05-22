PREPARE_ENV_VARS_SH="$1"

source $PREPARE_ENV_VARS_SH

echo $OSM_ENV_VARS_STR
gcloud composer environments update osm-to-bq-env \
  --location $OSM_TO_BQ_LOCATION \
  --update-env-variables=$OSM_ENV_VARS_STR