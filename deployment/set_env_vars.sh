$OSM_ENV_VARS_STR="$1"
gcloud composer environments update osm-to-bq-env \
  --location $OSM_TO_BQ_LOCATION \
  --update-env-variables=$OSM_ENV_VARS_STR