import json
import logging
import datetime
import time

import gcs_service
import file_service

OSM_ENTITIES = ["nodes", "ways", "relations"]


def create_processing_counter():
    return {key: 0 for key in OSM_ENTITIES}


def is_file_fresh(last_updated_timestamp, data_freshness_exp_days):
    db_freshness = datetime.timedelta(seconds=int(time.time()) - last_updated_timestamp)
    logging.info("Freshness: {}".format(db_freshness))
    return db_freshness < datetime.timedelta(days=data_freshness_exp_days)


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


def get_index_metadata_file_path(src_osm_name, num_db_shards):
    return file_service.file_name_without_ext(src_osm_name) + "_{}_index_shards.metadata.txt".format(num_db_shards)


def get_result_shard_metadata_file_path(src_osm_name, entity_type, index, num_results_shards):
    return file_service.file_name_without_ext(src_osm_name) + "_{}_{}_{}.metadata.txt".format(entity_type, index + 1,
                                                                                              num_results_shards)


def download_and_read_metadata_file(gcs_bucket, gcs_dir_name, src_osm_name, num_db_shards, num_results_shards):
    src_osm_file_name = file_service.file_name_from_path(src_osm_name)

    index_metadata_file_path = get_index_metadata_file_path(src_osm_file_name, num_db_shards)
    index_metadata_blob_name = gcs_dir_name + index_metadata_file_path
    if gcs_service.is_gcs_blob_exists(gcs_bucket, index_metadata_blob_name):
        gcs_service.from_gcs_to_local_file(gcs_bucket, index_metadata_blob_name, index_metadata_file_path)

    shards_metadata_files = {}
    for entity in OSM_ENTITIES:
        shards_metadata_files_by_entity = {}
        for index in range(num_results_shards):
            result_shard_metadata_file_path = get_result_shard_metadata_file_path(src_osm_file_name, entity, index,
                                                                                  num_results_shards)
            result_shard_metadata_blob_name = gcs_dir_name + result_shard_metadata_file_path
            if gcs_service.is_gcs_blob_exists(gcs_bucket, result_shard_metadata_blob_name):
                gcs_service.from_gcs_to_local_file(gcs_bucket, result_shard_metadata_blob_name,
                                                   result_shard_metadata_file_path)
            shards_metadata_files_by_entity[str(index)] = result_shard_metadata_file_path
        shards_metadata_files[entity] = shards_metadata_files_by_entity
    return ProcessingMetadata(index_metadata_file_path, shards_metadata_files)


def save_and_upload_metadata_to_gcs(metadata,
                                    dest_bucket,
                                    dest_dir_name,
                                    save_only_shard_by_entity_and_index=None,
                                    only_db_metadata=False):
    files_to_save = metadata.save_to_json_files(save_only_shard_by_entity_and_index, only_db_metadata)

    for file_to_save in files_to_save:
        timestamps_file_name = file_service.file_name_from_path(file_to_save)
        timestamps_file_blob_name = dest_dir_name + timestamps_file_name
        gcs_service.upload_file_to_gcs(file_to_save, dest_bucket, timestamps_file_blob_name)


class ProcessingMetadata(object):

    def __init__(self, index_metadata_file_path, shards_metadata_files):
        self.index_metadata_file_path = index_metadata_file_path
        self.shards_metadata_files = shards_metadata_files
        try:
            with open(index_metadata_file_path, "r") as f:
                metadata_json = json.load(f)
            self.elements_counter = MetadataCounter(metadata_json["elements_counter"])
            self.index_db_timestamps = FileTimestamps(metadata_json["index_db"])
        except Exception as e:
            logging.info(str(e))
            self.elements_counter = MetadataCounter()
            self.index_db_timestamps = FileTimestamps()

        self.shards_timestamps = {}
        for entity, shards_metadata_files_by_entity in shards_metadata_files.items():
            shards_timestamps_by_entity = {}
            for index_str, shards_metadata_file in shards_metadata_files_by_entity.items():
                try:
                    with open(shards_metadata_file, "r") as f:
                        metadata_json = json.load(f)
                    shards_timestamps_by_entity[index_str] = FileTimestamps(metadata_json)
                except Exception as e:
                    logging.info(str(e))
                    shards_timestamps_by_entity[index_str] = FileTimestamps()
            self.shards_timestamps[entity] = shards_timestamps_by_entity

    def update_db_max_timestamp(self, db_max_timestamp):
        self.index_db_timestamps.update_max_timestamp(db_max_timestamp)

    def update_db_last_updated(self, db_last_updated):
        self.index_db_timestamps.update_last_updated(db_last_updated)

    def update_processing_counter(self, counter_dict):
        self.elements_counter.update(counter_dict)

    def get_min_history_results_last_updated_timestamp(self):
        return min([min([shard_timestamps.last_updated for shard_index_str, shard_timestamps in
                         shards_timestamps_by_entity.items()]) for entity, shards_timestamps_by_entity in
                    self.shards_timestamps.items()])

    def get_history_results_max_timestamps(self):
        last_elements_timestamps = {}
        for entity, shards_timestamps_by_entity in self.shards_timestamps.items():
            last_elements_timestamps[entity] = {shard_index_str: shard_timestamps.max_timestamp for
                                                shard_index_str, shard_timestamps in
                                                shards_timestamps_by_entity.items()}
        return last_elements_timestamps

    def update_history_result_timestamps(self, entity_type, shard_index):
        self.shards_timestamps[entity_type][str(shard_index)].update_max_timestamp(
            self.index_db_timestamps.max_timestamp)
        self.shards_timestamps[entity_type][str(shard_index)].update_last_updated(
            int(time.time()))

    def save_to_json_files(self, specific_history_results_shards_to_save=None, only_db_metadata=False):
        files_to_save = []
        if not specific_history_results_shards_to_save:
            self.save_db_metadata(files_to_save)
        if not only_db_metadata:
            for entity, shards_timestamps_by_entity in self.shards_timestamps.items():
                for shard_index_str, shard_timestamps in shards_timestamps_by_entity.items():
                    if not specific_history_results_shards_to_save or (
                            entity == specific_history_results_shards_to_save[0] and int(shard_index_str) ==
                            int(specific_history_results_shards_to_save[1])):
                        shard_file = self.shards_metadata_files[entity][shard_index_str]
                        with open(shard_file, "w") as f:
                            json.dump(shard_timestamps.to_dict(), f)
                        files_to_save.append(shard_file)
        return files_to_save

    def save_db_metadata(self, files_to_save):
        with open(self.index_metadata_file_path, "w") as f:
            json.dump({"elements_counter": self.elements_counter.to_dict(),
                       "index_db": self.index_db_timestamps.to_dict()}, f)
            files_to_save.append(self.index_metadata_file_path)
        return files_to_save

    def to_dict(self):
        history_results = {}
        for entity, shards_timestamps_by_entity in self.shards_timestamps.items():
            history_results[entity] = {shard_index_str: shard_timestamps.to_dict() for shard_index_str, shard_timestamps
                                       in
                                       shards_timestamps_by_entity.items()}
        return {"elements_counter": self.elements_counter.to_dict(),
                "index_db": self.index_db_timestamps.to_dict(),
                "history_results": history_results}


class MetadataCounter(object):

    def __init__(self, counter_dict=None):
        if counter_dict:
            self.counter = {entity: counter_dict[entity] for entity in OSM_ENTITIES}
        else:
            self.counter = {entity: 0 for entity in OSM_ENTITIES}

    def update(self, counter):
        self.counter = counter

    def to_dict(self):
        return self.counter


class FileTimestamps(object):

    def __init__(self, timestamps_dict=None):
        if timestamps_dict:
            self.max_timestamp = timestamps_dict["max_timestamp"]
            self.last_updated = timestamps_dict["last_updated"]
        else:
            self.max_timestamp = 0
            self.last_updated = 0

    def update_max_timestamp(self, max_timestamp):
        self.max_timestamp = max_timestamp

    def update_last_updated(self, last_updated):
        self.last_updated = last_updated

    def to_dict(self):
        return {"max_timestamp": self.max_timestamp, "last_updated": self.last_updated}
