import datetime
import os
import airflow
import json

from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.operators import gcs_to_bq

from utils import gcs_utils

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

project_id = os.environ.get('PROJECT_ID')
bq_dataset_to_export = os.environ.get('BQ_DATASET_TO_EXPORT')
src_osm_gcs_uri = os.environ.get('SRC_OSM_GCS_URI')

osm_to_features_gke_pod_requested_memory = os.environ.get('OSM_TO_FEATURES_GKE_POD_REQUESTED_MEMORY')

# nodes_ways_relations_dir_gcs_uri = os.environ.get('NODES_WAYS_RELATIONS_DIR_GCS_URI')
# osm_to_nodes_ways_relations_image = os.environ.get('OSM_TO_NODES_WAYS_RELATIONS_IMAGE')
osm_converter_with_history_index_image = "gcr.io/gcp-pdp-osm-dev/osm_converter_with_history_index"
converted_osm_dir_gcs_uri="gs://gcp-pdp-osm-dev-work-historyeast1/converted/"
additional_gke_pool = os.environ.get('ADDITIONAL_GKE_POOL')

generate_layers_image = os.environ.get('GENERATE_LAYERS_IMAGE')
test_osm_gcs_uri = os.environ.get('TEST_OSM_GCS_URI')

feature_union_bq_table_name = "feature_union"

local_data_dir_path = "/home/airflow/gcs/dags/"

default_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': YESTERDAY,
}

max_bad_records_for_bq_export = 10000

with airflow.DAG(
        'osm_to_big_query_history',
        'catchup=False',
        default_args=default_args,
        schedule_interval=None) as dag:

    def file_to_json(file_path):
        with open(file_path) as f:
            json_dict = json.load(f)
        return json_dict


    def file_to_text(file_path):
        with open(file_path) as f:
            return "".join(f.readlines())


    def create_gke_affinity_with_pool_name(pool_name):
        return {'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        'values': [pool_name]
                    }]
                }]
            }
        }}


    src_osm_gcs_uri = test_osm_gcs_uri if test_osm_gcs_uri else "gs://{}/{}".format('{{ dag_run.conf.bucket }}',
                                                                                    '{{ dag_run.conf.name }}')
    # TASK #4. osm_converter_with_history_index
    convert_osm_with_history_index = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='convert-osm-with-history-index',
        name='convert-osm-with-history-index',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id,
                  'SRC_OSM_GCS_URI': src_osm_gcs_uri,
                  'CONVERTED_OSM_DIR_GCS_URI': converted_osm_dir_gcs_uri,
                  'NUM_THREADS': "8"},
        image=osm_converter_with_history_index_image,
        affinity=create_gke_affinity_with_pool_name(additional_gke_pool)
    )

    # TASK #5.N. nodes_ways_relations_to_bq
    nodes_ways_relations_elements = ["nodes", "ways", "relations"]
    nodes_ways_relations_tasks_data = []

    schemas = [file_to_json(local_data_dir_path + 'schemas/{}_table_schema.json'.format(element))
               for element in nodes_ways_relations_elements]

    elements_and_schemas = [(nodes_ways_relations_elements[i], schemas[i])
                            for i in range(len(nodes_ways_relations_elements))]
    schema = file_to_json(local_data_dir_path + 'schemas/simple_table_schema.json')
    src_nodes_ways_relations_gcs_bucket, src_nodes_ways_relations_gcs_dir = gcs_utils.parse_uri_to_bucket_and_filename(
        converted_osm_dir_gcs_uri)
    jsonl_file_names_format = src_nodes_ways_relations_gcs_dir + '{}.jsonl'

    for element_and_schema in elements_and_schemas:
        element, schema = element_and_schema
        task_id = element + '_json_to_bq'
        source_object = jsonl_file_names_format.format(element)
        destination_dataset_table = '{}.{}'.format(bq_dataset_to_export, element)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=src_nodes_ways_relations_gcs_bucket,
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=schema,
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=max_bad_records_for_bq_export,
            dag=dag)
        nodes_ways_relations_tasks_data.append((task, element, destination_dataset_table))

    # TASK #6. generate_layers
    generate_layers = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='generate-layers',
        name='generate-layers',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id,
                  'BQ_DATASET_TO_EXPORT': bq_dataset_to_export,
                  'MODE': 'history'},
        image=generate_layers_image,
        affinity=create_gke_affinity_with_pool_name(additional_gke_pool))

    # Graph building
    nodes_ways_relations_tasks = [nodes_ways_relations_tasks_item[0]
                                  for nodes_ways_relations_tasks_item in nodes_ways_relations_tasks_data]
    convert_osm_with_history_index.set_downstream(nodes_ways_relations_tasks)
    generate_layers.set_upstream(nodes_ways_relations_tasks)
