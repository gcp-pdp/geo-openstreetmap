import datetime

import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators import kubernetes_pod_operator

START = datetime.datetime(2020, 5, 13, 13, 50, 0)


SRC_OSM_LINK = 'https://osm.kewl.lu/luxembourg.osm/luxembourg.osm.pbf'
OSM_BUCKET = 'gs://osm-dev-276509--osm-data/'

default_args = {
    'owner': 'pseveryn',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': START,
}

with airflow.DAG(
        'osm_to_bq_naive_new',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(minutes=15)) as dag:

    copy_to_gcs_cmds = []
    copy_to_gcs_cmds.append('wget {}'.format(SRC_OSM_LINK))
    filename = SRC_OSM_LINK.split("/")[-1]
    copy_to_gcs_cmds.append('gsutil cp {} {}'.format(filename, OSM_BUCKET))
    copy_to_gcs_cmds.append('ls -lh')
    copy_to_gcs_cmds.append('rm {}'.format(filename))

    # Print the dag_run id from the Airflow logs
    copy_to_gcs = bash_operator.BashOperator(
        task_id='copy_to_gcs', bash_command="\n".join(copy_to_gcs_cmds),
        dag=dag)

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

    copy_to_gcs >> osm_to_json
