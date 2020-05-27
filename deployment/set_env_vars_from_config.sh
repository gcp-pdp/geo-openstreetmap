CONFIG_FILE="$1"

OSM_URL=$(cat $CONFIG_FILE | jq -r '.osm_url')
OSM_MD5_URL=$(cat $CONFIG_FILE | jq -r '.osm_md5_url')
GCS_WORKING_BUCKET=$(cat $CONFIG_FILE | jq -r '.gcs_working_bucket')
PROJECT_ID=$(cat $CONFIG_FILE | jq -r '.project_id')
GCS_TRANSFER_BUCKET=$(cat $CONFIG_FILE | jq -r '.gcs_transfer_bucket')
BQ_DATASET_TO_EXPORT=$(cat $CONFIG_FILE | jq -r '.bq_dataset_to_export')
SRC_OSM_GCS_URI=$(cat $CONFIG_FILE | jq -r '.src_osm_gcs_uri')
FEATURES_DIR_GCS_URI=$(cat $CONFIG_FILE | jq -r '.features_dir_gcs_uri')
NODES_WAYS_RELATIONS_DIR_GCS_URI=$(cat $CONFIG_FILE | jq -r '.nodes_ways_relations_dir_gcs_uri')
OSM_TO_FEATURES_IMAGE=$(cat $CONFIG_FILE | jq -r '.osm_to_features_image')
OSM_TO_NODES_WAYS_RELATIONS_IMAGE=$(cat $CONFIG_FILE | jq -r '.osm_to_nodes_ways_relations_image')
GENERATE_LAYERS_IMAGE=$(cat $CONFIG_FILE | jq -r '.generate_layers_image')
#COMPOSER_CLIENT_ID=$(cat $CONFIG_FILE | jq -r '.composer_client_id')
#COMPOSER_WEBSERVER_ID=$(cat $CONFIG_FILE | jq -r '.composer_webserver_id')

OSM_ENV_VARS_STR=\
"OSM_URL=$OSM_URL,\
OSM_MD5_URL=$OSM_MD5_URL,\
GCS_WORKING_BUCKET=$GCS_WORKING_BUCKET,\
PROJECT_ID=$PROJECT_ID,\
GCS_TRANSFER_BUCKET=$GCS_TRANSFER_BUCKET,\
OSM_TO_FEATURES_IMAGE=$OSM_TO_FEATURES_IMAGE,\
OSM_TO_NODES_WAYS_RELATIONS_IMAGE=$OSM_TO_NODES_WAYS_RELATIONS_IMAGE,\
GENERATE_LAYERS_IMAGE=$GENERATE_LAYERS_IMAGE,\
BQ_DATASET_TO_EXPORT=$BQ_DATASET_TO_EXPORT,\
SRC_OSM_GCS_URI=$SRC_OSM_GCS_URI,\
FEATURES_DIR_GCS_URI=$FEATURES_DIR_GCS_URI,\
NODES_WAYS_RELATIONS_DIR_GCS_URI=$NODES_WAYS_RELATIONS_DIR_GCS_URI"

echo $OSM_ENV_VARS_STR

gcloud composer environments update osm-to-bq-env \
  --location $OSM_TO_BQ_LOCATION \
  --update-env-variables=$OSM_ENV_VARS_STR