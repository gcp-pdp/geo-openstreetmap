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

def create_transfer_job_dict(project_id, list_url, transfer_bucket):
    now_datetime = datetime.datetime.utcnow()
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

project_id="osm-dev-276509"
list_url="https://storage.googleapis.com/osm-dev-276509--osm-data/gsc_transfer_index/index.tsv"
transfer_bucket="osm-data-transfer"

job_data = create_transfer_job_dict(project_id, list_url, transfer_bucket)
execute_transfer_job(job_data)