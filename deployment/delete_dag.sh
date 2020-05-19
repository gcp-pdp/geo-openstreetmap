DAG_NAME="$1"

gcloud composer environments storage dags delete \
  --environment osm-to-bq-env  --location $OSM_TO_BQ_LOCATION \
  $DAG_NAME
