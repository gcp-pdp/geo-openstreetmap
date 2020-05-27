import datetime
import os
import airflow
import json
import logging

from airflow.contrib.operators import kubernetes_pod_operator

from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_operator

from utils import bq_utils

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

project_id = os.environ.get('PROJECT_ID')
bq_dataset_to_export = os.environ.get('BQ_DATASET_TO_EXPORT')
src_osm_gcs_uri = os.environ.get('SRC_OSM_GCS_URI')

features_dir_gcs_uri = os.environ.get('FEATURES_DIR_GCS_URI')
nodes_ways_relations_dir_gcs_uri = os.environ.get('NODES_WAYS_RELATIONS_DIR_GCS_URI')

osm_to_features_image = os.environ.get('OSM_TO_FEATURES_IMAGE')
osm_to_nodes_ways_relations_image = os.environ.get('OSM_TO_NODES_WAYS_RELATIONS_IMAGE')
generate_layers_image = os.environ.get('GENERATE_LAYERS_IMAGE')

features = ["points", "lines", "multilinestrings", "multipolygons", "other_relations"]
feature_union_bq_table_name = "feature_union"

local_data_dir_path = "/home/airflow/gcs/dags/"

default_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': YESTERDAY,
}

with airflow.DAG(
        'osm_to_big_query',
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


    def parse_uri_to_bucket_and_filename(file_path):
        """Divides file uri to bucket name and file name"""
        path_parts = file_path.split("//")
        if len(path_parts) >= 2:
            main_part = path_parts[1]
            if "/" in main_part:
                divide_index = main_part.index("/")
                bucket_name = main_part[:divide_index]
                file_name = main_part[divide_index + 1 - len(main_part):]

                return bucket_name, file_name
        return "", ""


    # TASK #1. osm_to_features
    osm_to_features = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='osm-to-features',
        name='osm-to-features',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'SRC_OSM_GCS_URI': src_osm_gcs_uri, 'FEATURES_DIR_GCS_URI': features_dir_gcs_uri},
        image=osm_to_features_image)

    # TASK #2.N. {}_feature_json_to_bq
    features_tasks_data = []
    nodes_schema = file_to_json(local_data_dir_path + 'schemas/features_table_schema.json')
    src_features_gcs_bucket, src_features_gcs_dir = parse_uri_to_bucket_and_filename(features_dir_gcs_uri)
    jsonl_file_names_format = src_features_gcs_dir + 'feature-{}.geojson.csv.jsonl'

    for feature in features:
        task_id = feature + '_feature_json_to_bq'
        source_object = jsonl_file_names_format.format(feature)
        destination_dataset_table = '{}.feature_{}'.format(bq_dataset_to_export, feature)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=src_features_gcs_bucket,
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=nodes_schema,
            write_disposition='WRITE_TRUNCATE',
            dag=dag)
        features_tasks_data.append((task, feature, destination_dataset_table))

    # TASK #3. feature_union
    create_features_part_format = file_to_text(local_data_dir_path + 'sql/create_features_part_format.sql')
    create_features_queries = [create_features_part_format.format(task_tuple[1], project_id, task_tuple[2])
                               for task_tuple in features_tasks_data]
    feature_union_query = bq_utils.union_queries(create_features_queries)

    destination_table = "{}.{}".format(bq_dataset_to_export, feature_union_bq_table_name)
    feature_union_task = bigquery_operator.BigQueryOperator(
        task_id='feature_union',
        bql=feature_union_query,
        destination_dataset_table=destination_table,
        write_disposition='WRITE_TRUNCATE',
        use_legacy_sql=False
    )

    # TASK #4. osm_to_nodes_ways_relations
    osm_to_nodes_ways_relations = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='osm-to-nodes-ways-relations',
        name='osm-to-nodes-ways-relations',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id, 'SRC_OSM_GCS_URI': src_osm_gcs_uri,
                  'NODES_WAYS_RELATIONS_DIR_GCS_URI': nodes_ways_relations_dir_gcs_uri},
        image=osm_to_nodes_ways_relations_image)

    # TASK #5.N. nodes_ways_relations_to_bq
    nodes_ways_relations_elements = ["nodes", "ways", "relations"]
    nodes_ways_relations_tasks_data = []
    schemas = [file_to_json(local_data_dir_path + 'schemas/{}_table_schema.json'.format(element))
               for element in nodes_ways_relations_elements]

    elements_and_schemas = [(nodes_ways_relations_elements[i], schemas[i])
                            for i in range(len(nodes_ways_relations_elements))]
    src_nodes_ways_relations_gcs_bucket, src_nodes_ways_relations_gcs_dir = parse_uri_to_bucket_and_filename(
        nodes_ways_relations_dir_gcs_uri)
    jsonl_file_names_format = src_nodes_ways_relations_gcs_dir + '{}.json'

    for element_and_schema in elements_and_schemas:
        element, schema = element_and_schema

        task_id = element + '_json_to_bq'
        source_object = jsonl_file_names_format.format(element)
        destination_dataset_table = '{}.{}'.format(bq_dataset_to_export, element)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=src_features_gcs_bucket,
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=schema,
            write_disposition='WRITE_TRUNCATE',
            dag=dag)
        nodes_ways_relations_tasks_data.append((task, element, destination_dataset_table))

    # TASK #6. generate_layers
    generate_layers = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='generate-layers',
        name='generate-layers',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'PROJECT_ID': project_id, 'BQ_DATASET_TO_EXPORT': bq_dataset_to_export},
        image=generate_layers_image)

    feature_tasks = [feature_tasks_item[0] for feature_tasks_item in features_tasks_data]
    nodes_ways_relations_tasks = [nodes_ways_relations_tasks_item[0]
                                  for nodes_ways_relations_tasks_item in nodes_ways_relations_tasks_data]

    osm_to_features.set_downstream(feature_tasks)
    feature_union_task.set_upstream(feature_tasks)

    osm_to_nodes_ways_relations.set_upstream(feature_union_task)
    osm_to_nodes_ways_relations.set_downstream(nodes_ways_relations_tasks)
    generate_layers.set_upstream(nodes_ways_relations_tasks)
