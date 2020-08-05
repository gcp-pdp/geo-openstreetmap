import json
import logging
import datetime
import time

import gcs_service
import file_service


def create_processing_counter(entities):
    return {key: 0 for key in entities}


def metadata_to_file(file_path, processing_counter, max_timestamp_db, max_timestamp_history,
                     last_updated_db, last_updated_history):
    with open(file_path, "w") as f:
        json.dump(
            {"counter": processing_counter, "db": {"max_timestamp": max_timestamp_db, "last_updated": last_updated_db},
             "history": {"max_timestamp": max_timestamp_history, "last_updated": last_updated_history}}, f)
    return file_path


def get_values_from_metadata(metadata, entities):
    max_timestamp_db = 0
    max_timestamp_history = 0
    last_updated_db = 0
    last_updated_history = 0
    processing_counter = create_processing_counter(entities)

    if metadata:
        if "counter" in metadata:
            processing_counter = metadata["counter"]
        if "db" in metadata:
            if "max_timestamp" in metadata["db"]:
                max_timestamp_db = metadata["db"]["max_timestamp"]
            if "last_updated" in metadata["db"]:
                last_updated_db = metadata["db"]["last_updated"]
        if "history" in metadata:
            if "max_timestamp" in metadata["history"]:
                max_timestamp_history = metadata["history"]["max_timestamp"]
            if "last_updated" in metadata["history"]:
                last_updated_history = metadata["history"]["last_updated"]
    return max_timestamp_db, max_timestamp_history, last_updated_db, last_updated_history, processing_counter


def download_and_read_metadata_file(timestamps_file_path, dest_bucket, dest_dir_name):
    timestamps_file_name = file_service.file_name_from_path(timestamps_file_path)
    timestamps_file_blob_name = dest_dir_name + timestamps_file_name
    if gcs_service.is_gcs_blob_exists(dest_bucket, timestamps_file_blob_name):
        gcs_service.from_gcs_to_local_file(dest_bucket, timestamps_file_blob_name, timestamps_file_path)
        return metadata_from_file(timestamps_file_path)
    else:
        return {}


def get_metadata_file_path(db_name, num_db_shards):
    return file_service.file_name_without_ext(db_name) + "_{}_index_shards.metadata.txt".format(num_db_shards)


def metadata_from_file(file_path):
    try:
        with open(file_path, "r") as f:
            metadata_json = json.load(f)
        return metadata_json
    except Exception as e:
        logging.info(str(e))
        return {}


def upload_metadata_to_gcs(timestamps_file_path, indexing_processing_counter,
                           max_timestamp_db, max_timestamp_history,
                           last_updated_db, last_updated_history,
                           dest_bucket, dest_dir_name):
    metadata_to_file(timestamps_file_path,
                     indexing_processing_counter,
                     max_timestamp_db, max_timestamp_history,
                     last_updated_db, last_updated_history)

    timestamps_file_name = file_service.file_name_from_path(timestamps_file_path)
    timestamps_file_blob_name = dest_dir_name + timestamps_file_name
    gcs_service.upload_file_to_gcs(timestamps_file_path, dest_bucket, timestamps_file_blob_name)


def is_file_fresh(last_updated_timestamp, data_freshness_exp_days):
    db_freshness = datetime.timedelta(seconds=int(time.time()) - last_updated_timestamp)
    logging.info("Freshness: {}".format(db_freshness))
    return db_freshness < datetime.timedelta(days=data_freshness_exp_days)


def merge_if_db_needed_and_upload_to_gcs(osm_index, dbs, index_db_file_path, merge_db_shards, db_exists,
                                         converted_results_bucket, converted_results_gcs_dir):
    if merge_db_shards:
        dbs_to_merge = list(dbs.values())
        osm_index.merge_dbs(index_db_file_path, dbs_to_merge, db_exists)
        gcs_service.upload_file_to_gcs(index_db_file_path, converted_results_bucket,
                                       converted_results_gcs_dir + file_service.file_name_from_path(index_db_file_path))
        return {0: index_db_file_path}
    else:
        for shard_index, db_file_path in dbs.items():
            gcs_service.upload_file_to_gcs(db_file_path, converted_results_bucket,
                                           converted_results_gcs_dir + file_service.file_name_from_path(db_file_path))
        return dbs


def download_db_if_exists(dbs_file_paths,
                          dest_bucket,
                          dest_dir_name):
    db_gcs_and_local_paths = []
    for db_file_path in dbs_file_paths:
        db_name = file_service.file_name_from_path(db_file_path)
        db_blob_name = dest_dir_name + db_name

        if not gcs_service.is_gcs_blob_exists(dest_bucket, db_blob_name):
            return False
        else:
            db_gcs_and_local_paths.append((db_blob_name, db_file_path))

    for db_blob_name, db_file_path in db_gcs_and_local_paths:
        gcs_service.from_gcs_to_local_file(dest_bucket, db_blob_name, db_file_path)
    return True
