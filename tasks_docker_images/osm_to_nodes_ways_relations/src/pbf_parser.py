import osmium
import logging
import json
import argparse
import os
import errno
import time

from datetime import datetime
from google.cloud import storage


def osm_entity_to_dict(osm_entity):
    all_tags = [{"key": tag.k, "value": tag.v} for tag in osm_entity.tags]
    return {"id": osm_entity.id, "all_tags": all_tags}


def osm_entity_to_dict_full(osm_entity):
    base_dict = osm_entity_to_dict(osm_entity)
    base_dict.update({
        "version": osm_entity.version,
        "username": osm_entity.user,
        "changeset": osm_entity.changeset,
        "visible": osm_entity.visible,
        "osm_timestamp": int(datetime.timestamp(osm_entity.timestamp)),
    })
    return base_dict


def osm_entity_node_dict(osm_node_entity):
    base_dict = osm_entity_to_dict_full(osm_node_entity)
    base_dict["latitude"] = osm_node_entity.location.lat
    base_dict["longitude"] = osm_node_entity.location.lon
    return base_dict


def osm_entity_way_dict(osm_way_entity):
    base_dict = osm_entity_to_dict_full(osm_way_entity)
    base_dict["nodes"] = [{"id": node.ref} for node in osm_way_entity.nodes]
    return base_dict


def osm_entity_relation_dict(osm_relation_entity):
    base_dict = osm_entity_to_dict_full(osm_relation_entity)
    base_dict["members"] = [{"type": member.type, "id": member.id, "role": member.role}
                            for member in iter(osm_relation_entity.members)]
    return base_dict


class CustomHandler(osmium.SimpleHandler):

    def __init__(self, files_dict):
        osmium.SimpleHandler.__init__(self)
        self.entities_out_files_dict = files_dict
        self.processing_counter = 0

        self.last_log_time = time.time()

    def log_processing(self, entity_type):
        self.processing_counter = self.processing_counter + 1
        if self.processing_counter % 1000000 == 0:
            logging.info(entity_type + " " + str(self.processing_counter) + " " + str(time.time() - self.last_log_time))
            self.last_log_time = time.time()

    def node(self, node):
        self.log_processing("node")
        node_dict = osm_entity_to_dict(node)
        entities_out_files_dict["nodes"].write(json.dumps(node_dict) + "\n")

    def way(self, way):
        self.log_processing("way")
        way_dict = osm_entity_to_dict(way)
        entities_out_files_dict["ways"].write(json.dumps(way_dict) + "\n")

    def relation(self, relation):
        self.log_processing("relation")
        relation_dict = osm_entity_to_dict(relation)
        entities_out_files_dict["relations"].write(json.dumps(relation_dict) + "\n")


def make_dir_for_file_if_not_exists(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


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
    return "", ""


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("src_pbf_file_uri", help="The source PBF file to be converted")
    parser.add_argument("dest_gcs_dir", help="URI of GCS dir to save result files")

    args = parser.parse_args()

    src_bucket, src_name = parse_uri_to_bucket_and_filename(args.src_pbf_file_uri)

    data_dir = os.environ['DATA_DIR']
    dest_local_path = data_dir + "planet.osm.pbf"
    make_dir_for_file_if_not_exists(dest_local_path)
    from_gcs_to_local_file(src_bucket, src_name, dest_local_path)

    entities = ["nodes", "ways", "relations"]

    entities_out_files_dict = {}
    results_local_paths = []
    for entity in entities:
        path = data_dir + "{}.jsonl".format(entity)
        results_local_paths.append(path)

        make_dir_for_file_if_not_exists(path)
        entities_out_files_dict[entity] = open(path, "w")

    logging.info("Creating {} files".format(str(results_local_paths)))
    simple_handler = CustomHandler(entities_out_files_dict)
    simple_handler.apply_file(dest_local_path)

    for entity, out_file in entities_out_files_dict.items():
        out_file.close()

    dest_bucket, dest_dir_name = parse_uri_to_bucket_and_filename(args.dest_gcs_dir)
    for path in results_local_paths:
        dest_file_gcs_name = dest_dir_name + path.split("/")[-1]
        upload_file_to_gcs(path, dest_bucket, dest_file_gcs_name)
