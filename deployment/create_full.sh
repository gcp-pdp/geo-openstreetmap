##!/bin/bash
OSM_URL="$1"
OSM_MD5_URL="$2"
REGION_LOCATION="$3"
SUFFIX="$4"

ADDT_SN_CORES="$5"
ADDT_SN_DISK_SIZE="$6"

ADDT_MN_CORES="$7"
ADDT_MN_DISK_SIZE="$8"
ADDT_MN_NODES="$9"

MODE="$10"

PROJECT_ID=`gcloud config get-value project`

TRANSFER_BUCKET_NAME=${PROJECT_ID}-transfer-${SUFFIX}
gsutil mb gs://${TRANSFER_BUCKET_NAME}/

WORK_BUCKET_NAME=${PROJECT_ID}-work-${SUFFIX}
gsutil mb gs://${WORK_BUCKET_NAME}/

BQ_DATASET=osm_to_bq_${SUFFIX}
bq mk ${PROJECT_ID}:${BQ_DATASET}

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

COMPOSER_ENV_NAME=osm-to-bq-${SUFFIX}
gcloud composer environments create $COMPOSER_ENV_NAME \
    --location $REGION_LOCATION \
    --node-count 6

GKE_CLUSTER_FULL_NAME=$(gcloud composer environments describe $COMPOSER_ENV_NAME \
        --location $REGION_LOCATION --format json | jq -r '.config.gkeCluster')
GKE_CLUSTER_NAME=$(echo $GKE_CLUSTER_FULL_NAME | awk -F/ '{print $6}')
GKE_ZONE=$(echo $GKE_CLUSTER_FULL_NAME | awk -F/ '{print $4}')


ADDT_SN_POOL_NUM_CORES=$ADDT_SN_CORES
ADDT_SN_POOL_DISK_SIZE=$ADDT_SN_DISK_SIZE
ADDT_SN_POOL_NAME=osm-addt-sn-pool-${SUFFIX}
ADDT_SN_POOL_MACHINE_TYPE=n1-highmem-$ADDT_SN_POOL_NUM_CORES
ADDT_SN_POOL_NUM_NODES=1
gcloud container node-pools create $ADDT_SN_POOL_NAME \
    --cluster $GKE_CLUSTER_NAME \
    --project $PROJECT_ID \
    --zone $GKE_ZONE \
    --machine-type $ADDT_SN_POOL_MACHINE_TYPE \
    --num-nodes $ADDT_SN_POOL_NUM_NODES \
    --disk-size $ADDT_SN_POOL_DISK_SIZE \
    --disk-type pd-ssd \
    --scopes gke-default,storage-rw,bigquery

ADDT_SN_POOL_MAX_NUM_TREADS=$((ADDT_SN_POOL_NUM_CORES/4))


if [ "$MODE" = "planet" ]
then
  OSM_TO_FEATURES_POOL_NUM_CORES=16
  OSM_TO_FEATURES_POOL_NAME=osm-to-features-pool-${SUFFIX}
  OSM_TO_FEATURES_POOL_MACHINE_TYPE=n1-highmem-$OSM_TO_FEATURES_POOL_NUM_CORES
  OSM_TO_FEATURES_POOL_NUM_NODES=2
  OSM_TO_FEATURES_POOL_DISK_SIZE=1200
  gcloud container node-pools create $OSM_TO_FEATURES_POOL_NAME \
    --cluster $GKE_CLUSTER_NAME \
    --project $PROJECT_ID \
    --zone $GKE_ZONE \
    --machine-type $OSM_TO_FEATURES_POOL_MACHINE_TYPE \
    --num-nodes $OSM_TO_FEATURES_POOL_NUM_NODES \
    --disk-size $OSM_TO_FEATURES_POOL_DISK_SIZE \
    --scopes gke-default,storage-rw
  OSM_TO_FEATURES_POD_REQUESTED_MEMORY=$((OSM_TO_FEATURES_POOL_NUM_CORES*5))G
else
  ADDT_MN_POOL_NUM_CORES=$ADDT_MN_CORES
  ADDT_MN_POOL_DISK_SIZE=$ADDT_MN_DISK_SIZE
  ADDT_MN_POOL_NAME=osm-addt-mn-pool-${SUFFIX}
  ADDT_MN_POOL_MACHINE_TYPE=n1-highmem-$ADDT_MN_POOL_NUM_CORES
  ADDT_MN_POOL_NUM_NODES=$ADDT_MN_NODES
  gcloud container node-pools create $ADDT_MN_POOL_NAME \
      --cluster $GKE_CLUSTER_NAME \
      --project $PROJECT_ID \
      --zone $GKE_ZONE \
      --machine-type $ADDT_MN_POOL_MACHINE_TYPE \
      --num-nodes $ADDT_MN_POOL_NUM_NODES \
      --disk-size $ADDT_MN_POOL_DISK_SIZE \
      --disk-type pd-ssd \
      --scopes gke-default,storage-rw,bigquery
  ADDT_MN_POD_REQUESTED_MEMORY=$((ADDT_MN_POOL_NUM_CORES*5))G
fi

CONFIG_FILE=deployment/config/config_${SUFFIX}.json
python3 deployment/config/generate_config.py $CONFIG_FILE \
    --project_id=$PROJECT_ID \
    --osm_url=$OSM_URL \
    --osm_md5_url=$OSM_MD5_URL \
    --gcs_transfer_bucket=$TRANSFER_BUCKET_NAME \
    --gcs_work_bucket=$WORK_BUCKET_NAME \
    --transfer_index_files_gcs_uri=gs://$WORK_BUCKET_NAME/gsc_transfer_index/ \
    --osm_to_features_image=$OSM_TO_FEATURES_IMAGE \
    --osm_to_nodes_ways_relations_image=$OSM_TO_NODES_WAYS_RELATIONS_IMAGE \
    --generate_layers_image=$GENERATE_LAYERS_IMAGE \
    --osm_converter_with_history_index_image=$OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE \
    --osm_to_features_gke_pool=$OSM_TO_FEATURES_POOL_NAME \
    --osm_to_features_gke_pod_requested_memory=$OSM_TO_FEATURES_POD_REQUESTED_MEMORY \
    --addt_sn_gke_pool=$ADDT_SN_POOL_NAME \
    --addt_sn_gke_pool_max_num_treads=$ADDT_SN_POOL_MAX_NUM_TREADS \
    --addt_mn_gke_pool=$ADDT_MN_POOL_NAME \
    --addt_mn_gke_pool_num_nodes=$ADDT_MN_POOL_NUM_NODES \
    --addt_mn_pod_requested_memory=$ADDT_MN_POD_REQUESTED_MEMORY \
    --bq_dataset_to_export=$BQ_DATASET

deployment/config/set_env_vars_from_config.sh $CONFIG_FILE $COMPOSER_ENV_NAME $REGION_LOCATION

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

if [ "$MODE" = "planet" ]
then
  DAGS_PATH='dags/osm_to_big_query_planet.py dags/transfer_src_file.py  dags/*/'
else
  DAGS_PATH='dags/osm_to_big_query_history.py dags/transfer_src_file.py  dags/*/'
fi
for DAG_ELEMENT in $DAGS_PATH; do
  deployment/upload_dags_files.sh $DAG_ELEMENT $COMPOSER_ENV_NAME $REGION_LOCATION
done
