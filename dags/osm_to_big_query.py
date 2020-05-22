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
gcs_data_bucket = os.environ.get('GCS_DATA_BUCKET')
osm_to_json_image = os.environ.get('OSM_TO_JSON_IMAGE')
bq_dataset_to_export = os.environ.get('BQ_DATASET_TO_EXPORT')
src_osm_gcs_uri = os.environ.get('SRC_OSM_GCS_URI')

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

    # TASK #1. osm_to_json
    osm_to_json = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='osm-to-json',
        name='osm-to-json',
        namespace='default',
        image_pull_policy='Always',
        env_vars={'SRC_OSM_GCS_URI': src_osm_gcs_uri},
        image=osm_to_json_image)

    # TASK #2.N. {}_feature_json_to_bq
    tasks_data = []
    nodes_schema = file_to_json(local_data_dir_path + 'schemas/features_table_schema.json')
    jsonl_file_names_format = 'json/feature-{}.geojson.csv.jsonl'

    for feature in features:
        task_id = feature + '_feature_json_to_bq'
        source_object = jsonl_file_names_format.format(feature)
        destination_dataset_table = '{}.feature_{}'.format(bq_dataset_to_export, feature)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket=gcs_data_bucket,
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=nodes_schema,
            write_disposition='WRITE_TRUNCATE',
            dag=dag)
        tasks_data.append((task, feature, destination_dataset_table))

    # TASK #3. feature_union
    create_features_part_format = file_to_text(local_data_dir_path + 'sql/create_features_part_format.sql')
    create_features_queries = [create_features_part_format.format(task_tuple[1], project_id, task_tuple[2])
                               for task_tuple in tasks_data]
    feature_union_query = bq_utils.union_queries(create_features_queries)

    destination_table = "{}.{}".format(bq_dataset_to_export, feature_union_bq_table_name)
    feature_union_task = bigquery_operator.BigQueryOperator(
        task_id='feature_union',
        bql=feature_union_query,
        destination_dataset_table=destination_table,
        use_legacy_sql=False
    )

    tasks = [task[0] for task in tasks_data]
    osm_to_json.set_downstream(tasks)
    feature_union_task.set_upstream(tasks)
