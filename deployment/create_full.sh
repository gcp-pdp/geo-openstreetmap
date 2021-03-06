##!/bin/bash

# 1. Read intput parameters
OSM_URL="$1"
OSM_MD5_URL="$2"
REGION_LOCATION="$3"
ZONE="$4"
SUFFIX="$5"

BASE_COMPOSER_CLUSTER_MACHINE_TYPE="$6"
BASE_COMPOSER_CLUSTER_NODES="$7"

ADDT_SN_CORES="$8"
ADDT_SN_DISK_SIZE="$9"

ADDT_MN_CORES="${10}"
ADDT_MN_DISK_SIZE="${11}"
ADDT_MN_NODES="${12}"

MODE="${13}"

# 2. Print all parameters
for PARAM in "$@"; do
  echo "$PARAM"
done

# 3. Retrieve PROJECT_ID
PROJECT_ID=`gcloud config get-value project`

# 4. Create GCS buckets
TRANSFER_BUCKET_NAME=${PROJECT_ID}-transfer-${SUFFIX}
gsutil mb gs://${TRANSFER_BUCKET_NAME}/

WORK_BUCKET_NAME=${PROJECT_ID}-work-${SUFFIX}
gsutil mb gs://${WORK_BUCKET_NAME}/

# 5. Create BigQuery dataset
BQ_DATASET_SHORT=osm_to_bq_${SUFFIX}
BQ_DATASET=${PROJECT_ID}.${BQ_DATASET_SHORT}
bq mk ${PROJECT_ID}:${BQ_DATASET_SHORT}
#TODO temp
#BQ_DATASET=bigquery-public-data.geo_openstreetmap

# 6. Build and push to Container Registry Docker containers
IMAGE_HOSTNAME=gcr.io

GENERATE_LAYERS_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/generate_layers_${SUFFIX}
docker build -t $GENERATE_LAYERS_IMAGE tasks_docker_images/generate_layers/
docker push $GENERATE_LAYERS_IMAGE

if [ "$MODE" = "planet" ]
then
  OSM_TO_FEATURES_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/osm_to_features_${SUFFIX}
  docker build -t $OSM_TO_FEATURES_IMAGE tasks_docker_images/osm_to_features/
  docker push $OSM_TO_FEATURES_IMAGE

  OSM_TO_NODES_WAYS_RELATIONS_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/osm_to_nodes_ways_relations_${SUFFIX}
  docker build -t $OSM_TO_NODES_WAYS_RELATIONS_IMAGE tasks_docker_images/osm_to_nodes_ways_relations/
  docker push $OSM_TO_NODES_WAYS_RELATIONS_IMAGE
else
  OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/osm_converter_with_history_index_${SUFFIX}
  docker build -t $OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE tasks_docker_images/osm_converter_with_history_index/
  docker push $OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE
fi

# 7. Create Cloud Composer environment
COMPOSER_ENV_NAME=osm-to-bq-${SUFFIX}
gcloud composer environments create $COMPOSER_ENV_NAME \
    --location $REGION_LOCATION \
    --zone $ZONE \
    --node-count $BASE_COMPOSER_CLUSTER_NODES \
    --machine-type $BASE_COMPOSER_CLUSTER_MACHINE_TYPE \
    --airflow-configs=broker_transport_options-visibility_timeout=2592000

# 8. Retrieve Cloud Composer environment's params
GKE_CLUSTER_FULL_NAME=$(gcloud composer environments describe $COMPOSER_ENV_NAME \
        --location $REGION_LOCATION --format json | jq -r '.config.gkeCluster')
GKE_CLUSTER_NAME=$(echo $GKE_CLUSTER_FULL_NAME | awk -F/ '{print $6}')

# 9. Define additional Kubernetes clusters parameters
ADDT_SN_POOL_NUM_CORES=$ADDT_SN_CORES
ADDT_SN_POOL_DISK_SIZE=$ADDT_SN_DISK_SIZE
ADDT_SN_POOL_NAME=osm-addt-sn-pool-${SUFFIX}
ADDT_SN_POOL_MACHINE_TYPE=n1-highmem-$ADDT_SN_POOL_NUM_CORES
ADDT_SN_POOL_NUM_NODES=1
ADDT_SN_POOL_MAX_NUM_TREADS=$((ADDT_SN_POOL_NUM_CORES/4))


ADDT_MN_POOL_NUM_CORES=$ADDT_MN_CORES
ADDT_MN_POOL_DISK_SIZE=$ADDT_MN_DISK_SIZE
ADDT_MN_POOL_NAME=osm-addt-mn-pool-${SUFFIX}
ADDT_MN_POOL_MACHINE_TYPE=n1-highmem-$ADDT_MN_POOL_NUM_CORES
ADDT_MN_POOL_NUM_NODES=$ADDT_MN_NODES
ADDT_MN_POD_REQUESTED_MEMORY=$((ADDT_MN_POOL_NUM_CORES*4))G

# 10. Build config file with Cloud Composer env vars
CONFIG_FILE=deployment/config/config_${SUFFIX}.json
python3 deployment/config/generate_config.py $CONFIG_FILE \
    --project_id=$PROJECT_ID \
    --zone=$ZONE \
    --osm_url=$OSM_URL \
    --osm_md5_url=$OSM_MD5_URL \
    --gcs_transfer_bucket=$TRANSFER_BUCKET_NAME \
    --gcs_work_bucket=$WORK_BUCKET_NAME \
    --transfer_index_files_gcs_uri=gs://$WORK_BUCKET_NAME/gsc_transfer_index/ \
    --osm_to_features_image=$OSM_TO_FEATURES_IMAGE \
    --osm_to_nodes_ways_relations_image=$OSM_TO_NODES_WAYS_RELATIONS_IMAGE \
    --generate_layers_image=$GENERATE_LAYERS_IMAGE \
    --osm_converter_with_history_index_image=$OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE \
    --gke_main_cluster_name=$GKE_CLUSTER_NAME \
    --addt_sn_gke_pool=$ADDT_SN_POOL_NAME \
    --addt_sn_pool_machine_type=$ADDT_SN_POOL_MACHINE_TYPE \
    --addt_sn_pool_disk_size=$ADDT_SN_POOL_DISK_SIZE \
    --addt_sn_pool_num_nodes=$ADDT_SN_POOL_NUM_NODES \
    --addt_sn_pool_max_num_treads=$ADDT_SN_POOL_MAX_NUM_TREADS \
    --addt_mn_gke_pool=$ADDT_MN_POOL_NAME \
    --addt_mn_pool_machine_type=$ADDT_MN_POOL_MACHINE_TYPE \
    --addt_mn_pool_disk_size=$ADDT_MN_POOL_DISK_SIZE \
    --addt_mn_pool_num_nodes=$ADDT_MN_POOL_NUM_NODES \
    --addt_mn_pod_requested_memory=$ADDT_MN_POD_REQUESTED_MEMORY \
    --bq_dataset_to_export=$BQ_DATASET

# 11. Deploy Cloud Composer env vars
deployment/config/set_env_vars_from_config.sh $CONFIG_FILE $COMPOSER_ENV_NAME $REGION_LOCATION

# 12. Crete Cloud Function for triggering main DAG
COMPOSER_CLIENT_ID=$(python3 utils/get_client_id.py $PROJECT_ID $REGION_LOCATION $COMPOSER_ENV_NAME)
COMPOSER_WEBSERVER_ID=$(gcloud composer environments describe $COMPOSER_ENV_NAME \
        --location $REGION_LOCATION --format json | \
        jq -r '.config.airflowUri' | \
        awk -F/ '{print $3}' | \
        cut -d '.' -f1)
DAG_NAME=osm_to_big_query_${MODE}

TRIGGER_FUNCTION_NAME=trigger_osm_to_big_query_dg_gcf_${SUFFIX}
gcloud functions deploy $TRIGGER_FUNCTION_NAME \
    --source triggering/trigger_osm_to_big_query_dg_gcf \
    --entry-point trigger_dag \
    --runtime python37 \
    --trigger-resource $TRANSFER_BUCKET_NAME \
    --trigger-event google.storage.object.finalize \
    --set-env-vars COMPOSER_CLIENT_ID=$COMPOSER_CLIENT_ID,COMPOSER_WEBSERVER_ID=$COMPOSER_WEBSERVER_ID,DAG_NAME=$DAG_NAME

# 13. Deploy DAG files and its dependencies
if [ "$MODE" = "planet" ]
then
  DAGS_PATH='dags/osm_to_big_query_planet.py dags/transfer_src_file.py  dags/*/'
else
  DAGS_PATH='dags/osm_to_big_query_history.py dags/transfer_src_file.py  dags/*/'
fi
for DAG_ELEMENT in $DAGS_PATH; do
  deployment/upload_dags_files.sh $DAG_ELEMENT $COMPOSER_ENV_NAME $REGION_LOCATION
done
