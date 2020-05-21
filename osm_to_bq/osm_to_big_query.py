import datetime
import os
import airflow

from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.sensors.pubsub_sensor import PubSubPullSensor

from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import bigquery_operator

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

project_id = os.environ.get('PROJECT_ID')
subscription = os.environ.get('DATA_UPDATE_SUBSCRIPTION')

default_args = {
    'owner': 'pseveryn',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': YESTERDAY,
}

with airflow.DAG(
        'osm_to_big_query',
        'catchup=False',
        default_args=default_args,
        schedule_interval=None) as dag:

    osm_to_json = kubernetes_pod_operator.KubernetesPodOperator(
        # The ID specified for the task.
        task_id='osm-to-json',
        # Name of task you want to run, used to generate Pod ID.
        name='osm-to-json',
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        namespace='default',
        image_pull_policy='Always',
        # Docker image specified. Defaults to hub.docker.com, but any fully
        # qualified URLs will point to a custom repository. Supports private
        # gcr.io images if the Composer Environment is under the same
        # project-id as the gcr.io images and the service account that Composer
        # uses has permission to access the Google Container Registry
        # (the default service account has permission)
        image='eu.gcr.io/osm-dev-276509/osm_to_json')

    tasks_data = []
    features = ["points", "lines", "multilinestrings", "multipolygons", "other_relations"]
    nodes_schema = [
        {
            "description": None,
            "name": "osm_id",
            "type": "INTEGER"
        },
        {
            "description": None,
            "name": "osm_version",
            "type": "INTEGER"
        },
        {
            "description": None,
            "name": "osm_way_id",
            "type": "INTEGER"
        },
        {
            "description": "Last-modified timestamp for this object.",
            "name": "osm_timestamp",
            "type": "TIMESTAMP"
        },
        {
            "description": "GEOGRAPHY-encoded point",
            "name": "geometry",
            "type": "GEOGRAPHY"
        },
        {
            "description": "Unstructured key=value attributes for this object.",
            "fields": [
                {
                    "description": "Attribute key.",
                    "name": "key",
                    "type": "STRING"
                },
                {
                    "description": "Attribute value.",
                    "name": "value",
                    "type": "STRING"
                }
            ],
            "mode": "REPEATED",
            "name": "all_tags",
            "type": "RECORD"
        }]
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
                        FROM `osm-dev-276509.{}`
                    """
    # bql = create_part
    bql = ""
    for task_tuple in tasks_data:
        _, feature, dataset_table = task_tuple
        if tasks_data.index(task_tuple) > 0:
            bql += """
                UNION ALL
                """
        bql += table_part.format(feature, dataset_table)

    feature_union_task = bigquery_operator.BigQueryOperator(
        task_id='feature_union',
        bql=bql,
        destination_dataset_table='osm_to_bq.feature_union',
        use_legacy_sql=False
    )

    tasks = [task[0] for task in tasks_data]
    osm_to_json.set_downstream(tasks)
    feature_union_task.set_upstream(tasks)

