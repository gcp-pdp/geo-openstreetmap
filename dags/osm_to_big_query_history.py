import datetime
import os
import airflow
import json

from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.operators import gcs_to_bq

from utils import gcs_utils

year_start = datetime.datetime(2020, 1, 1)

project_id = os.environ.get('PROJECT_ID')
bq_dataset_to_export = os.environ.get('BQ_DATASET_TO_EXPORT')
src_osm_gcs_uri = os.environ.get('SRC_OSM_GCS_URI')

gcs_work_bucket = os.environ.get('GCS_WORK_BUCKET')

osm_converter_with_history_index_image = os.environ.get('OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE')
addt_sn_gke_pool = os.environ.get('ADDT_SN_GKE_POOL')
addt_sn_gke_pool_max_num_treads = os.environ.get('ADDT_SN_GKE_POOL_MAX_NUM_TREADS')

addt_mn_gke_pool = os.environ.get('ADDT_MN_GKE_POOL')
addt_mn_gke_pool_num_nodes = os.environ.get('ADDT_MN_GKE_POOL_NUM_NODES')

generate_layers_image = os.environ.get('GENERATE_LAYERS_IMAGE')
test_osm_gcs_uri = os.environ.get('TEST_OSM_GCS_URI')

num_index_db_shards = 80

converted_osm_dir_gcs_uri = "gs://{}/converted/".format(gcs_work_bucket)
index_db_and_metadata_dir_gcs_uri = "gs://{}/index_db_and_metadata/".format(gcs_work_bucket)
feature_union_bq_table_name = "feature_union"

addt_mn_pod_requested_memory = os.environ.get('ADDT_MN_POD_REQUESTED_MEMORY')

local_data_dir_path = "/home/airflow/gcs/dags/"
startup_timeout_seconds = 1200

default_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=15),
    'start_date': year_start,
}

max_bad_records_for_bq_export = 10000

with airflow.DAG(
        'osm_to_big_query_history',
        catchup=False,
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
    create_index_mode_additional_args = "--create_index_mode"
    update_history_index = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='update-history-index',
        name='update-history-index',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id,
                  'SRC_OSM_GCS_URI': src_osm_gcs_uri,
                  'CONVERTED_OSM_DIR_GCS_URI': converted_osm_dir_gcs_uri,
                  'INDEX_DB_AND_METADATA_DIR_GCS_URI': index_db_and_metadata_dir_gcs_uri,
                  'NUM_DB_SHARDS': str(num_index_db_shards),
                  'NUM_THREADS': addt_sn_gke_pool_max_num_treads,
                  'ADDITIONAL_ARGS': create_index_mode_additional_args},
        image=osm_converter_with_history_index_image,
        startup_timeout_seconds=startup_timeout_seconds,
        affinity=create_gke_affinity_with_pool_name(addt_sn_gke_pool),
        execution_timeout=datetime.timedelta(days=3)
    )

    generate_history_data_json_tasks = []
    for index in range(int(addt_mn_gke_pool_num_nodes)):
        generate_history_additional_args = "--history_processing_pool_index {} --history_processing_pool_size {}" \
            .format(index, addt_mn_gke_pool_num_nodes)
        generate_history_data_json_task = kubernetes_pod_operator.KubernetesPodOperator(
            task_id='generate-history-data-json-{}-{}'.format(index + 1, addt_mn_gke_pool_num_nodes),
            name='generate-history-data-json-{}-{}'.format(index + 1, addt_mn_gke_pool_num_nodes),
            namespace='default',
            image_pull_policy='Always',
            env_vars={'PROJECT_ID': project_id,
                      'SRC_OSM_GCS_URI': src_osm_gcs_uri,
                      'CONVERTED_OSM_DIR_GCS_URI': converted_osm_dir_gcs_uri,
                      'INDEX_DB_AND_METADATA_DIR_GCS_URI': index_db_and_metadata_dir_gcs_uri,
                      'NUM_DB_SHARDS': str(num_index_db_shards),
                      'NUM_THREADS': addt_sn_gke_pool_max_num_treads,
                      'ADDITIONAL_ARGS': generate_history_additional_args},
            image=osm_converter_with_history_index_image,
            startup_timeout_seconds=startup_timeout_seconds,
            resources={"request_memory": addt_mn_pod_requested_memory},
            affinity=create_gke_affinity_with_pool_name(addt_mn_gke_pool),
            execution_timeout=datetime.timedelta(days=15)
        )
        generate_history_data_json_tasks.append(generate_history_data_json_task)

    generate_history_data_json_tasks_with_downstream = []
    for index, generate_history_data_json_task in enumerate(generate_history_data_json_tasks):
        # TASK #5.N. nodes_ways_relations_to_bq
        nodes_ways_relations_elements = ["nodes", "ways", "relations"]
        nodes_ways_relations_tasks_data = []

        schemas = [file_to_json(local_data_dir_path + 'schemas/{}_table_schema.json'.format(element))
                   for element in nodes_ways_relations_elements]

        elements_and_schemas = [(nodes_ways_relations_elements[i], schemas[i])
                                for i in range(len(nodes_ways_relations_elements))]
        schema = file_to_json(local_data_dir_path + 'schemas/simple_table_schema.json')
        src_nodes_ways_relations_gcs_bucket, src_nodes_ways_relations_gcs_dir = \
            gcs_utils.parse_uri_to_bucket_and_filename(converted_osm_dir_gcs_uri)
        jsonl_file_names_format = src_nodes_ways_relations_gcs_dir + '{}_{}.jsonl'

        for element_and_schema in elements_and_schemas:
            element, schema = element_and_schema
            task_id = element + '_json_to_bq_{}_{}'.format(index + 1, addt_sn_gke_pool_max_num_treads)
            source_object = jsonl_file_names_format.format(element, index)
            destination_dataset_table = '{}.{}'.format(bq_dataset_to_export, element)

            task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
                task_id=task_id,
                bucket=src_nodes_ways_relations_gcs_bucket,
                source_objects=[source_object],
                source_format='NEWLINE_DELIMITED_JSON',
                destination_project_dataset_table=destination_dataset_table,
                schema_fields=schema,
                write_disposition='WRITE_APPEND',
                max_bad_records=max_bad_records_for_bq_export,
                dag=dag)
            nodes_ways_relations_tasks_data.append(task)
        generate_history_data_json_tasks_with_downstream.append(
            (generate_history_data_json_task, nodes_ways_relations_tasks_data))

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
        affinity=create_gke_affinity_with_pool_name(addt_sn_gke_pool),
        execution_timeout=datetime.timedelta(days=2))

    # Graph building
    update_history_index.set_downstream(generate_history_data_json_tasks)
    upload_to_bq_tasks = []
    for generate_history_data_json_task, downstream_upload_tasks in generate_history_data_json_tasks_with_downstream:
        upload_to_bq_tasks.extend(downstream_upload_tasks)
        generate_history_data_json_task.set_downstream(downstream_upload_tasks)
    generate_layers.set_upstream(upload_to_bq_tasks)
