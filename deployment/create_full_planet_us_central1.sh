#!/bin/bash
OSM_URL=https://storage.googleapis.com/gcp-pdp-osm-dev-work/planet-latest-singapore.pbf
OSM_MD5_URL=https://storage.googleapis.com/gcp-pdp-osm-dev-work/planet-latest-singapore.pbf.md5
REGION_LOCATION=us-central1
SUFFIX=planetuscentral1

PROJECT_ID=`gcloud config get-value project`

TRANSFER_BUCKET_NAME=${PROJECT_ID}-transfer-${SUFFIX}
#gsutil mb gs://${TRANSFER_BUCKET_NAME}/

WORK_BUCKET_NAME=${PROJECT_ID}-work-${SUFFIX}
#gsutil mb gs://${WORK_BUCKET_NAME}/


BQ_DATASET=osm_to_bq_${SUFFIX}
#bq mk ${PROJECT_ID}:${BQ_DATASET}

IMAGE_HOSTNAME=gcr.io

OSM_TO_FEATURES_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/osm_to_features_${SUFFIX}
#docker build -t $OSM_TO_FEATURES_IMAGE tasks_docker_images/osm_to_features/
#docker push $OSM_TO_FEATURES_IMAGE

OSM_TO_NODES_WAYS_RELATIONS_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/osm_to_nodes_ways_relations_${SUFFIX}
#docker build -t $OSM_TO_NODES_WAYS_RELATIONS_IMAGE tasks_docker_images/osm_to_nodes_ways_relations/
#docker push $OSM_TO_NODES_WAYS_RELATIONS_IMAGE

GENERATE_LAYERS_IMAGE=$IMAGE_HOSTNAME/$PROJECT_ID/generate_layers_${SUFFIX}
#docker build -t $GENERATE_LAYERS_IMAGE tasks_docker_images/generate_layers/
#docker push $GENERATE_LAYERS_IMAGE


COMPOSER_ENV_NAME=osm-to-bq-${SUFFIX}
#gcloud composer environments create $COMPOSER_ENV_NAME \
#    --location $REGION_LOCATION

#GKE_CLUSTER_FULL_NAME=$(gcloud composer environments describe $COMPOSER_ENV_NAME \
#        --location $REGION_LOCATION --format json | jq -r '.config.gkeCluster')
#GKE_CLUSTER_NAME=$(echo $GKE_CLUSTER_FULL_NAME | awk -F/ '{print $6}')
#GKE_ZONE=$(echo $GKE_CLUSTER_FULL_NAME | awk -F/ '{print $4}')


OSM_TO_FEATURES_POOL_NAME=osm-to-features-pool-${SUFFIX}
OSM_TO_FEATURES_POOL_MACHINE_TYPE=n1-highmem-32
OSM_TO_FEATURES_POOL_NUM_NODES=2
OSM_TO_FEATURES_POOL_DISK_SIZE=1200
#gcloud container node-pools create $OSM_TO_FEATURES_POOL_NAME \
#    --cluster $GKE_CLUSTER_NAME \
#    --project $PROJECT_ID \
#    --zone $GKE_ZONE \
#    --machine-type $OSM_TO_FEATURES_POOL_MACHINE_TYPE \
#    --num-nodes $OSM_TO_FEATURES_POOL_NUM_NODES \
#    --disk-size $OSM_TO_FEATURES_POOL_DISK_SIZE \
#    --scopes gke-default,storage-rw

OSM_TO_FEATURES_POD_REQUESTED_MEMORY=85G

ADDITIONAL_POOL_NAME=osm-to-bq-addit-pool-${SUFFIX}
ADDITIONAL_POOL_MACHINE_TYPE=n1-highmem-4
ADDITIONAL_POOL_NUM_NODES=1
ADDITIONAL_POOL_DISK_SIZE=1200
#gcloud container node-pools create $ADDITIONAL_POOL_NAME \
#    --cluster $GKE_CLUSTER_NAME \
#    --project $PROJECT_ID \
#    --zone $GKE_ZONE \
#    --machine-type $ADDITIONAL_POOL_MACHINE_TYPE \
#    --num-nodes $ADDITIONAL_POOL_NUM_NODES \
#    --disk-size $ADDITIONAL_POOL_DISK_SIZE \
#    --scopes gke-default,storage-rw,bigquery


CONFIG_FILE=deployment/config/config-${SUFFIX}.json
#python3 deployment/config/generate_config.py $CONFIG_FILE \
#    --project_id=$PROJECT_ID \
#    --osm_url=$OSM_URL \
#    --osm_md5_url=$OSM_MD5_URL \
#    --gcs_transfer_bucket=$TRANSFER_BUCKET_NAME \
#    --features_dir_gcs_uri=gs://$WORK_BUCKET_NAME/features/ \
#    --nodes_ways_relations_dir_gcs_uri=gs://$WORK_BUCKET_NAME/nodes_ways_relations/ \
#    --transfer_index_files_dir_gcs_uri=gs://$WORK_BUCKET_NAME/gsc_transfer_index/ \
#    --osm_to_features_image=$OSM_TO_FEATURES_IMAGE \
#    --osm_to_nodes_ways_relations_image=$OSM_TO_NODES_WAYS_RELATIONS_IMAGE \
#    --osm_to_features_gke_pool=$OSM_TO_FEATURES_POOL_NAME \
#    --osm_to_features_gke_pod_requested_memory=$OSM_TO_FEATURES_POD_REQUESTED_MEMORY \
#    --additional_gke_pool=$ADDITIONAL_POOL_NAME \
#    --generate_layers_image=$GENERATE_LAYERS_IMAGE \
#    --bq_dataset_to_export=$BQ_DATASET

deployment/config/set_env_vars_from_config.sh $CONFIG_FILE $COMPOSER_ENV_NAME $REGION_LOCATION

#COMPOSER_CLIENT_ID=$(python3 utils/get_client_id.py $PROJECT_ID $REGION_LOCATION $COMPOSER_ENV_NAME)
#COMPOSER_WEBSERVER_ID=$(gcloud composer environments describe $COMPOSER_ENV_NAME \
#        --location $REGION_LOCATION --format json | \
#        jq -r '.config.airflowUri' | \
#        awk -F/ '{print $3}' | \
#        cut -d '.' -f1)
#DAG_NAME=osm_to_big_query
#
#TRIGGER_FUNCTION_NAME=trigger_osm_to_big_query_dg_gcf_${SUFFIX}
#gcloud functions deploy $TRIGGER_FUNCTION_NAME \
#    --source triggering/trigger_osm_to_big_query_dg_gcf \
#    --entry-point trigger_dag \
#    --runtime python37 \
#    --trigger-resource $TRANSFER_BUCKET_NAME \
#    --trigger-event google.storage.object.finalize \
#    --set-env-vars COMPOSER_CLIENT_ID=$COMPOSER_CLIENT_ID,COMPOSER_WEBSERVER_ID=$COMPOSER_WEBSERVER_ID,DAG_NAME=$DAG_NAME
#
#DAGS_PATH=dags/*
#for DAG_ELEMENT in $DAGS_PATH; do
#  deployment/upload_dags_files.sh $DAG_ELEMENT $COMPOSER_ENV_NAME $REGION_LOCATION
#done