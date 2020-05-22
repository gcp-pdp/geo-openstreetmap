DAG_PATH="$1"

gcloud composer environments storage dags import \
  --environment osm-to-bq-env --location $OSM_TO_BQ_LOCATION \
  --source $DAG_PATH