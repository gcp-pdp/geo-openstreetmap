import airflow
import os
import logging
import datetime
import json

import googleapiclient.discovery

from urllib import request
from airflow.operators import python_operator
from google.cloud import storage

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

OSM_TRANSFER_INDEX_FILE_NAME = "osm_transfer_index.tsv"
OSM_TRANSFER_INDEX_GCS_NAME = "gsc_transfer_index/"+OSM_TRANSFER_INDEX_FILE_NAME


project_id = os.environ.get('PROJECT_ID')
osm_url = os.environ.get('OSM_URL')
osm_md5_url = os.environ.get('OSM_MD5_URL')
gcs_data_bucket = os.environ.get('GCS_DATA_BUCKET')
gcs_transfer_bucket = os.environ.get('GCS_TRANSFER_BUCKET')

default_args = {
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'start_date': YESTERDAY,
}

with airflow.DAG(
        'transferring_src_osm_file',
        'catchup=False',
        default_args=default_args,
        schedule_interval=None) as dag:

    def transfer_to_gcs():
        logging.info([osm_url, osm_md5_url, gcs_data_bucket])

        md5_file_lines = read_file_lines_from_url(osm_md5_url)
        md5_hash = get_md5_hash_from_md5_file_lines(md5_file_lines)
        logging.info(md5_hash)

        content_length = get_content_length_from_url(osm_url)
        logging.info(content_length)

        osm_transfer_index_file_name = create_transfer_index_tsv(OSM_TRANSFER_INDEX_FILE_NAME,
                                                                 osm_url,
                                                                 content_length,
                                                                 md5_hash)
        list_url = upload_file_to_gcs_as_public(osm_transfer_index_file_name,
                                                gcs_data_bucket,
                                                OSM_TRANSFER_INDEX_GCS_NAME)

        job_dict = create_transfer_job_dict(project_id, list_url, gcs_transfer_bucket)
        execute_transfer_job(job_dict)

    def read_file_lines_from_url(url):
        data = request.urlopen(url)
        return [byte_str_to_str(line) for line in data]

    def byte_str_to_str(byte_str):
        return byte_str.decode("utf-8")

    def get_md5_hash_from_md5_file_lines(lines):
        first_line = lines[0]
        return first_line.split()[0]

    def get_content_length_from_url(url):
        data = request.urlopen(url)
        meta = data.info()
        return meta.get(name="Content-Length")

    def create_transfer_index_tsv(osm_transfer_index_file_name, url, content_length, md5_hash):
        header_line = "TsvHttpData-1.0"
        lines = [header_line + "\n", "\t".join([url, content_length, md5_hash])]

        with open(osm_transfer_index_file_name, "w") as osm_transfer_index_file:
            osm_transfer_index_file.writelines(lines)
        return osm_transfer_index_file_name

    def upload_file_to_gcs_as_public(osm_transfer_index_file_name, gcs_data_bucket, osm_transfer_index_gcs_name):
        client = storage.Client()

        bucket = client.get_bucket(gcs_data_bucket)
        dest_blob = bucket.blob(osm_transfer_index_gcs_name)
        dest_blob.upload_from_filename(osm_transfer_index_file_name)

        dest_blob.make_public()

        return dest_blob.public_url

    def bucket_name_and_file_name_from_gcs_uri(gcs_uri):

        gcs_uri_without_gs_part = gcs_uri.split("//")[-1]
        uri_parts = gcs_uri_without_gs_part.split("/")

        return uri_parts[0], "/".join(uri_parts[1:])


    def create_transfer_job_dict(project_id, list_url, transfer_bucket):
        now_datetime = datetime.datetime.now()
        transfer_datetime = now_datetime + datetime.timedelta(minutes=10)

        job_description = "transfer--{}".format(transfer_datetime.strftime("%Y-%m-%d--%H-%M-%S"))
        job_name = "transferJobs/{}".format(job_description)

        transfer_date = {
            "day": transfer_datetime.day,
            "month": transfer_datetime.month,
            "year": transfer_datetime.year
        }
        transfer_time = {
            "hours": transfer_datetime.hour,
            "minutes": transfer_datetime.minute,
            "seconds": transfer_datetime.second
        }
        status = "ENABLED"
        transfer_job = {
            "name": job_name,
            "description":job_description,
            "transferSpec": {
                "httpDataSource": {
                    "listUrl": list_url
                },
                "gcsDataSink": {
                    "bucketName": transfer_bucket
                }
            },
            "projectId": project_id,
            "schedule": {
                "scheduleEndDate": transfer_date,
                "scheduleStartDate": transfer_date,
                "startTimeOfDay": transfer_time
            },
            "status": status
        }
        return transfer_job

    def execute_transfer_job(job_dict):
        storage_transfer = googleapiclient.discovery.build('storagetransfer', 'v1')
        logging.info('Requesting transferJob: {}'.format(
            job_dict))
        result = storage_transfer.transferJobs().create(body=job_dict).execute()
        logging.info('Returned transferJob: {}'.format(
            json.dumps(result, indent=4)))


    transferring_to_gcs = python_operator.PythonOperator(
        task_id='transferring_to_gcs',
        python_callable=transfer_to_gcs)
