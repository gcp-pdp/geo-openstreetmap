import osmium
import logging
import json
import argparse
import os
import errno

from datetime import datetime

from google.cloud import storage
from dataclasses import dataclass
from osmium.osm._osm import Node
from osmium.osm._osm import Way
from osmium.osm._osm import Relation
from osmium.osm._osm import OSMObject
from osmium.osm._osm import RelationMember



@dataclass
class OsmObjectDTO(object):
    id: int
    version: int
    username: str
    changeset: int
    visible: bool
    timestamp: int
    tags = []

    def __init__(self, osm_entity: OSMObject):
        self.id = osm_entity.id
        self.version = osm_entity.version
        self.username = osm_entity.user
        self.changeset = osm_entity.changeset
        self.visible = osm_entity.visible
        self.timestamp = int(datetime.timestamp(osm_entity.timestamp))
        self.tags = [(tag.k, tag.v) for tag in osm_entity.tags]

    def __dict__(self):
        tags_dict = [{"key": tag[0], "value": tag[1]} for tag in self.tags]
        return {"id": self.id, "version": self.version, "username": self.username, "changeset": self.changeset,
                "visible": self.visible, "osm_timestamp": self.timestamp, "all_tags": tags_dict}


@dataclass
class NodeDTO(OsmObjectDTO):
    latitude: float
    longitude: float

    def __init__(self, node_entity: Node):
        OsmObjectDTO.__init__(self, node_entity)
        self.latitude = node_entity.location.lat
        self.longitude = node_entity.location.lon

    def __dict__(self):
        dict_repr = super(NodeDTO, self).__dict__()
        dict_repr["latitude"] = self.latitude
        dict_repr["longitude"] = self.longitude
        return dict_repr


@dataclass
class WayDTO(OsmObjectDTO):
    nodes: list

    def __init__(self, way_entity: Way):
        OsmObjectDTO.__init__(self, way_entity)
        self.nodes = [node.ref for node in way_entity.nodes]

    def __dict__(self):
        dict_repr = super(WayDTO, self).__dict__()
        dict_repr["nodes"] = [{"id": node} for node in self.nodes]
        return dict_repr


@dataclass
class RelationDTO(OsmObjectDTO):
    members: list

    def __init__(self, relation_entity: Relation):
        OsmObjectDTO.__init__(self, relation_entity)
        self.members = [RelationMemberDTO(member) for member in iter(relation_entity.members)]

    def __dict__(self):
        dict_repr = super(RelationDTO, self).__dict__()
        dict_repr["members"] = [member.__dict__() for member in self.members]
        return dict_repr


@dataclass
class RelationMemberDTO(object):
    type: str
    id: int
    role: str

    def __init__(self, relation_entity: RelationMember):
        self.type = relation_entity.type
        self.id = relation_entity.ref
        self.role = relation_entity.role

    def __dict__(self):
        return {"type": self.type, "id": self.id, "role": self.role}


class CustomHandler(osmium.SimpleHandler):

    def __init__(self, nodes_file):
        osmium.SimpleHandler.__init__(self)
        self.entities_out_files_dict = entities_out_files_dict

    def node(self, node):
        node_dto = NodeDTO(node)
        entities_out_files_dict["nodes"].write(json.dumps(node_dto.__dict__()) + "\n")

    def way(self, way):
        way_dto = WayDTO(way)
        entities_out_files_dict["ways"].write(json.dumps(way_dto.__dict__()) + "\n")

    def relation(self, relation):
        relation_dto = RelationDTO(relation)
        entities_out_files_dict["relations"].write(json.dumps(relation_dto.__dict__()) + "\n")

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
    logging.info("Uploading of {} to gs://{}/{}...".format(filename, dest_bucket, destination_blob_name))
    blob.upload_from_filename(
        filename,
        content_type="text/plain")
    logging.info("Finished uploading of {} to gs://{}/{}".format(filename, dest_bucket, destination_blob_name))

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
    dest_local_path = os.environ['PROJECT_ID'] + "planet.osm.pbf"
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



