import datetime
import os
import airflow
import json

from airflow.contrib.operators import kubernetes_pod_operator

from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_operator

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

project_id = os.environ.get('PROJECT_ID')
subscription = os.environ.get('DATA_UPDATE_SUBSCRIPTION')
osm_to_json_image = os.environ.get('OSM_TO_JSON_IMAGE')

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

    def json_from_file(file_path):
        with open(file_path) as f:
            return json.load(f)

    osm_to_json = kubernetes_pod_operator.KubernetesPodOperator(
        task_id='osm-to-json',
        name='osm-to-json',
        namespace='default',
        image_pull_policy='Always',
        image=osm_to_json_image)

    tasks_data = []
    features = ["points", "lines", "multilinestrings", "multipolygons", "other_relations"]
    nodes_schema = json_from_file("schemas/features_table_schema.json")
    for feature in features:
        task_id = feature + '_feature_json_to_bq'
        source_object = 'json/feature-{}.geojson.csv.jsonl'.format(feature)
        destination_dataset_table = 'osm_to_bq.feature_{}'.format(feature)

        task = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id=task_id,
            bucket='osm-data-work',
            source_objects=[source_object],
            source_format='NEWLINE_DELIMITED_JSON',
            destination_project_dataset_table=destination_dataset_table,
            schema_fields=nodes_schema,
            write_disposition='WRITE_TRUNCATE',
            dag=dag)
        tasks_data.append((task, feature, destination_dataset_table))

    table_part = """
                    SELECT '{}' AS feature_type, osm_id, osm_way_id, osm_version, osm_timestamp, all_tags, geometry 
                        FROM `{}.{}`
                    """
    bql = ""
    for task_tuple in tasks_data:
        _, feature, dataset_table = task_tuple
        if tasks_data.index(task_tuple) > 0:
            bql += """
                UNION ALL
                """
        bql += table_part.format(feature, project_id, dataset_table)

    feature_union_task = bigquery_operator.BigQueryOperator(
        task_id='feature_union',
        bql=bql,
        destination_dataset_table='osm_to_bq.feature_union',
        use_legacy_sql=False
    )

    tasks = [task[0] for task in tasks_data]
    osm_to_json.set_downstream(tasks)
    feature_union_task.set_upstream(tasks)

