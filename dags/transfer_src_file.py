import airflow
import os
import logging
import datetime
import json
import base64
import binascii
import time

import googleapiclient.discovery

from urllib import request
from airflow.operators import python_operator
from google.cloud import storage

from utils import gcs_utils

YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

OSM_TRANSFER_INDEX_FILE_NAME_BASE = "osm_transfer_index"
OSM_TRANSFER_INDEX_FILE_NAME_EXT = ".tsv"
OSM_TRANSFER_INDEX_FILE_NAME = OSM_TRANSFER_INDEX_FILE_NAME_BASE + OSM_TRANSFER_INDEX_FILE_NAME_EXT


project_id = os.environ.get('PROJECT_ID')
osm_url = os.environ.get('OSM_URL')
osm_md5_url = os.environ.get('OSM_MD5_URL')
transfer_index_files_dir_gcs_uri = os.environ.get('TRANSFER_INDEX_FILES_GCS_URI')
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
        schedule_interval="@weekly") as dag:

    def transfer_to_gcs():
        md5_file_lines = read_file_lines_from_url(osm_md5_url)
        logging.info(md5_file_lines)

        md5_hex = get_md5_hash_from_md5_file_lines(md5_file_lines)
        logging.info(md5_hex)

        base64_md5_file_hash = md5_hex_to_base64(md5_hex)

        content_length = get_content_length_from_url(osm_url)
        logging.info(content_length)

        osm_transfer_index_file_name = create_transfer_index_tsv(OSM_TRANSFER_INDEX_FILE_NAME,
                                                                 osm_url,
                                                                 content_length,
                                                                 base64_md5_file_hash)
        index_gcs_bucket, index_gcs_dir = gcs_utils.parse_uri_to_bucket_and_filename(transfer_index_files_dir_gcs_uri)
        list_url = upload_file_to_gcs_as_public(osm_transfer_index_file_name,
                                                index_gcs_bucket,
                                                index_gcs_dir)

        job_dict = create_transfer_job_dict(project_id, list_url, gcs_transfer_bucket)
        execute_transfer_job(job_dict)

    def read_file_lines_from_url(url):
        logging.info(url)

        request.urlcleanup()
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

    def md5_hex_to_base64(md5_hex):
        return byte_str_to_str(to_base64(from_hex_to_binary(md5_hex)))

    def from_hex_to_binary(hex):
        return binascii.unhexlify(hex)

    def to_base64(byte_str):
        return base64.b64encode(byte_str)

    def create_transfer_index_tsv(osm_transfer_index_file_name, url, content_length, md5_hash):
        header_line = "TsvHttpData-1.0"
        lines = [header_line, "\t".join([url, content_length, md5_hash])]
        lines = [line+"\n" for line in lines]
        with open(osm_transfer_index_file_name, "w") as osm_transfer_index_file:
            osm_transfer_index_file.writelines(lines)
        return osm_transfer_index_file_name

    def upload_file_to_gcs_as_public(osm_transfer_index_file_name, gcs_data_bucket, osm_transfer_index_gcs_dir):
        client = storage.Client()

        osm_transfer_index_gcs_name = osm_transfer_index_gcs_dir \
                                      + add_timestamped_suffix(OSM_TRANSFER_INDEX_FILE_NAME_BASE) \
                                      + OSM_TRANSFER_INDEX_FILE_NAME_EXT
        bucket = client.get_bucket(gcs_data_bucket)
        dest_blob = bucket.blob(osm_transfer_index_gcs_name)
        dest_blob.upload_from_filename(osm_transfer_index_file_name)

        dest_blob.make_public()

        return dest_blob.public_url

    def add_timestamped_suffix(name):
        return name + "_" + str(time.time()).split(".")[0]

    def bucket_name_and_file_name_from_gcs_uri(gcs_uri):

        gcs_uri_without_gs_part = gcs_uri.split("//")[-1]
        uri_parts = gcs_uri_without_gs_part.split("/")

        return uri_parts[0], "/".join(uri_parts[1:])


    def create_transfer_job_dict(project_id, list_url, transfer_bucket):
        now_datetime = datetime.datetime.now()
        transfer_datetime = now_datetime + datetime.timedelta(minutes=3)

        job_description = "transfer--{}".format(transfer_datetime.strftime("%Y-%m-%d--%H-%M-%S"))
        job_name = "transferJobs/{}".format(job_description)
        overwrite_objects_already_existing_in_sink = True

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
            "description": job_description,
            "transferSpec": {
                "httpDataSource": {
                    "listUrl": list_url
                },
                "gcsDataSink": {
                    "bucketName": transfer_bucket
                },
                "transferOptions": {
                    "overwriteObjectsAlreadyExistingInSink":
                        overwrite_objects_already_existing_in_sink
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
