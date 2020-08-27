import logging
import os

from google.cloud import storage


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
        else:
            raise Exception("Wrong file_path format: {}".format(file_path))
    else:
        raise Exception("Wrong file_path format: {}".format(file_path))


def from_gcs_to_local_file(src_gcs_bucket, src_gcs_name, local_file_path):
    storage_client = storage.Client(os.environ['PROJECT_ID'])
    # Create a bucket object for our bucket
    bucket = storage_client.get_bucket(src_gcs_bucket)
    # Create a blob object from the filepath
    blob = bucket.blob(src_gcs_name)
    # Download the file to a destination
    logging.info("Downloading gs://{}/{} to {}...".format(src_gcs_bucket, src_gcs_name, local_file_path))
    blob.download_to_filename(local_file_path)
    logging.info("Successfully downloaded gs://{}/{} to {}".format(src_gcs_bucket, src_gcs_name, local_file_path))


def is_gcs_blob_exists(bucket, blob_name):
    storage_client = storage.Client(os.environ['PROJECT_ID'])
    # Create a bucket object for our bucket
    bucket = storage_client.get_bucket(bucket)
    # Create a blob object from the filepath
    blob = bucket.blob(blob_name)
    return blob.exists()


def upload_file_to_gcs(filename, destination_bucket_name, destination_blob_name):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    bucket = storage.Client().bucket(destination_bucket_name)
    blob = bucket.blob(destination_blob_name)
    logging.info("Uploading of {} to gs://{}/{}...".format(filename, destination_bucket_name, destination_blob_name))
    blob.upload_from_filename(
        filename,
        content_type="text/plain")
    logging.info(
        "Finished uploading of {} to gs://{}/{}".format(filename, destination_bucket_name, destination_blob_name))