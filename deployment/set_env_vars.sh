KEY_VALUE_CONCATTED="$1"

gcloud composer environments update osm-to-bq-env \
  --location $OSM_TO_BQ_LOCATION \
  --update-env-variables=$KEY_VALUE_CONCATTED