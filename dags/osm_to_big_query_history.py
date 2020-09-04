import datetime
import os
import airflow
import json

from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import gcs_delete_operator
from airflow.operators import python_operator

from utils import gcs_utils
from utils import metadata_manager
from utils import tasks_manager

year_start = datetime.datetime(2020, 1, 1)

project_id = os.environ.get('PROJECT_ID')
bq_dataset_to_export = os.environ.get('BQ_DATASET_TO_EXPORT')
src_osm_gcs_uri = os.environ.get('SRC_OSM_GCS_URI')

gcs_work_bucket = os.environ.get('GCS_WORK_BUCKET')

osm_converter_with_history_index_image = os.environ.get('OSM_CONVERTER_WITH_HISTORY_INDEX_IMAGE')

gke_main_cluster_name = os.environ.get('GKE_MAIN_CLUSTER_NAME')
gke_zone = os.environ.get('ZONE')

addt_sn_gke_pool = os.environ.get('ADDT_SN_GKE_POOL')
addt_sn_gke_pool_machine_type = os.environ.get('ADDT_SN_POOL_MACHINE_TYPE')
addt_sn_gke_pool_disk_size = os.environ.get('ADDT_SN_POOL_DISK_SIZE')
addt_sn_gke_pool_num_nodes = os.environ.get('ADDT_SN_POOL_NUM_NODES')
addt_sn_gke_pool_max_num_treads = os.environ.get('ADDT_SN_POOL_MAX_NUM_TREADS')

addt_mn_gke_pool = os.environ.get('ADDT_MN_GKE_POOL')
addt_mn_gke_pool_machine_type = os.environ.get('ADDT_MN_POOL_MACHINE_TYPE')
addt_mn_gke_pool_disk_size = os.environ.get('ADDT_MN_POOL_DISK_SIZE')
addt_mn_gke_pool_num_nodes = os.environ.get('ADDT_MN_POOL_NUM_NODES')
addt_mn_pod_requested_memory = os.environ.get('ADDT_MN_POD_REQUESTED_MEMORY')

generate_layers_image = os.environ.get('GENERATE_LAYERS_IMAGE')
test_osm_gcs_uri = os.environ.get('TEST_OSM_GCS_URI')

num_index_db_shards = 80

converted_osm_dir_gcs_uri = "gs://{}/converted/".format(gcs_work_bucket)
index_db_and_metadata_dir_gcs_uri = "gs://{}/index_db_and_metadata/".format(gcs_work_bucket)
feature_union_bq_table_name = "feature_union"

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


    def update_shard_timestamp(entity_type, shard_index, src_osm_uri, num_db_shards, num_results_shards):
        gcs_bucket, gcs_dir = gcs_utils.parse_uri_to_bucket_and_filename(index_db_and_metadata_dir_gcs_uri)

        metadata = metadata_manager.download_and_read_metadata_file(gcs_bucket, gcs_dir, src_osm_uri,
                                                                    int(num_db_shards), int(num_results_shards))
        metadata.update_history_result_timestamps(entity_type, shard_index)
        metadata_manager.save_and_upload_metadata_to_gcs(metadata, gcs_bucket, gcs_dir, (entity_type, shard_index))


    gke_config = tasks_manager.GKEConfig(project_id=project_id,
                                         cluster_name=gke_main_cluster_name,
                                         zone=gke_zone)

    src_osm_gcs_uri = test_osm_gcs_uri if test_osm_gcs_uri else "gs://{}/{}".format('{{ dag_run.conf.bucket }}',
                                                                                    '{{ dag_run.conf.name }}')

    # TASK #1. update-history-index
    sn_additional_pool_task_fabric = \
        tasks_manager.CreateAdditionalPoolTaskFabric(gke_config=gke_config,
                                                     pool_name=addt_sn_gke_pool,
                                                     machine_type=addt_sn_gke_pool_machine_type,
                                                     num_nodes=addt_sn_gke_pool_num_nodes,
                                                     disk_size=addt_sn_gke_pool_disk_size)
    create_sn_additional_pool_task = sn_additional_pool_task_fabric.create_task()

    # TASK #2. update-history-index
    create_index_mode_additional_args = "--create_index_mode"
    update_history_index_task = kubernetes_pod_operator.KubernetesPodOperator(
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

    # TASK #3. update-history-index
    mn_additional_pool_task_fabric = \
        tasks_manager.CreateAdditionalPoolTaskFabric(gke_config=gke_config,
                                                     pool_name=addt_mn_gke_pool,
                                                     machine_type=addt_mn_gke_pool_machine_type,
                                                     num_nodes=addt_mn_gke_pool_num_nodes,
                                                     disk_size=addt_mn_gke_pool_disk_size)
    create_mn_additional_pool_task = mn_additional_pool_task_fabric.create_task()

    # TASK #4.x. generate-history-data-json
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

    # TASK #5. generate_layers
    delete_mn_additional_pool_task_fabric = \
        tasks_manager.DeleteAdditionalPoolTaskFabric(gke_config=gke_config,
                                                     pool_name=addt_mn_gke_pool)
    delete_mn_additional_pool_task = delete_mn_additional_pool_task_fabric.create_task()

    # TASK #6.x. nodes_ways_relations_to_bq
    generate_history_data_json_tasks_with_downstream = []

    json_processing_task_chains = []
    for index, generate_history_data_json_task in enumerate(generate_history_data_json_tasks):
        nodes_ways_relations_elements = ["nodes", "ways", "relations"]
        json_to_bq_tasks = []

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
            task_id = element + '_json_to_bq_{}_{}'.format(index + 1, addt_mn_gke_pool_num_nodes)
            source_object = jsonl_file_names_format.format(element, index)
            destination_dataset_table = '{}.history_{}'.format(bq_dataset_to_export, element)

            json_to_bq_task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
                task_id=task_id,
                bucket=src_nodes_ways_relations_gcs_bucket,
                source_objects=[source_object],
                source_format='NEWLINE_DELIMITED_JSON',
                destination_project_dataset_table=destination_dataset_table,
                schema_fields=schema,
                write_disposition='WRITE_APPEND',
                max_bad_records=max_bad_records_for_bq_export,
                retries=5,
                dag=dag)
            json_to_bq_tasks.append(json_to_bq_task)
            remove_json_task = gcs_delete_operator.GoogleCloudStorageDeleteOperator(
                task_id='remove_json-{}-{}-{}'.format(element, index + 1, addt_mn_gke_pool_num_nodes),
                bucket_name=src_nodes_ways_relations_gcs_bucket,
                objects=[source_object])

            update_result_shard_timestamp_task = python_operator.PythonOperator(
                task_id='update-result-shard-timestamp-{}-{}-{}'.format(element, index + 1,
                                                                        addt_mn_gke_pool_num_nodes),
                python_callable=update_shard_timestamp,
                op_args=[element, index, src_osm_gcs_uri, num_index_db_shards, addt_mn_gke_pool_num_nodes],
                dag=dag)

            json_processing_task_chains.append((json_to_bq_tasks, remove_json_task, update_result_shard_timestamp_task))

        generate_history_data_json_tasks_with_downstream.append(
            (generate_history_data_json_task, json_to_bq_tasks))

    # TASK #7. generate_layers
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

    # TASK #8. delete_sn_additional_pool
    delete_sn_additional_pool_task_fabric = \
        tasks_manager.DeleteAdditionalPoolTaskFabric(gke_config=gke_config,
                                                     pool_name=addt_sn_gke_pool)
    delete_sn_additional_pool_task = delete_sn_additional_pool_task_fabric.create_task()

    # Building graph
    create_sn_additional_pool_task >> update_history_index_task >> create_mn_additional_pool_task
    create_mn_additional_pool_task >> generate_history_data_json_tasks

    for generate_history_data_json_task, json_to_bq_tasks in generate_history_data_json_tasks_with_downstream:
        generate_history_data_json_task >> json_to_bq_tasks

    for json_to_bq_task, delete_json_task, update_result_shard_timestamp_task in json_processing_task_chains:
        json_to_bq_task >> delete_json_task >> update_result_shard_timestamp_task

    update_result_shard_timestamp_tasks = [update_result_shard_timestamp_task
                                           for _, _, update_result_shard_timestamp_task in json_processing_task_chains]

    update_result_shard_timestamp_tasks >> generate_layers >> delete_sn_additional_pool_task

