import osmium
import logging
import os
import errno
import time
import json
import argparse
import multiprocessing

from datetime import datetime
from google.cloud import storage

import osm_obj_transformer
import osm_index
import osm_processing

from gdal import gdal_handler


class OsmHandler(osmium.SimpleHandler):

    def __init__(self, processing_counter, logging_range_count, pool_size=1, pool_index=0):
        osmium.SimpleHandler.__init__(self)

        self.processing_counter = processing_counter
        self.last_log_time = time.time()
        self.logging_range_count = logging_range_count
        self.current_entity_type = ""
        self.pool_index = pool_index
        self.pool_size = pool_size

    def is_item_index_for_current_thread(self):
        return self.processing_counter[self.current_entity_type] % self.pool_size == self.pool_index

    def log_processing(self):
        self.processing_counter[self.current_entity_type] = self.processing_counter[self.current_entity_type] + 1
        if self.processing_counter[self.current_entity_type] % self.logging_range_count == 0:
            logging.info(self.current_entity_type + " ({}/{}) ".format(self.pool_index+1, self.pool_size)
                         + str(self.processing_counter[self.current_entity_type])
                         + " " + str(time.time() - self.last_log_time))
            self.last_log_time = time.time()

    def node(self, node):
        self.current_entity_type = "nodes"
        self.log_processing()

    def way(self, way):
        self.current_entity_type = "ways"
        self.log_processing()

    def relation(self, relation):
        self.current_entity_type = "relations"
        self.log_processing()


class IndexCreator(OsmHandler):

    def __init__(self, osm_indexer, processing_counter,
                 pool_size=1, pool_index=0,
                 batch_size_to_commit=100000, logging_range_count=1000000):
        OsmHandler.__init__(self, processing_counter, logging_range_count, pool_size, pool_index)
        self.osm_indexer = osm_indexer
        self.batch_size_to_commit = batch_size_to_commit


    def node(self, node):
        OsmHandler.node(self, node)
        if self.is_item_index_for_current_thread():
            self.osm_indexer.add_node_to_index(osm_obj_transformer.osm_entity_node_dict(node, is_simplified=True))
            self.commit_if_needed()

    def way(self, way):
        OsmHandler.way(self, way)
        if self.is_item_index_for_current_thread():
            self.osm_indexer.add_way_to_index(osm_obj_transformer.osm_entity_way_dict(way, is_simplified=True))
            self.commit_if_needed()

    def relation(self, relation):
        OsmHandler.relation(self, relation)
        if self.is_item_index_for_current_thread():
            self.osm_indexer.add_relation_to_index(
                osm_obj_transformer.osm_entity_relation_dict(relation, is_simplified=True))
            self.commit_if_needed()

    def commit_if_needed(self):
        if self.processing_counter[self.current_entity_type] % self.batch_size_to_commit == 0:
            self.osm_indexer.save()


class HistoryHandler(OsmHandler):

    def __init__(self, osm_indexer, files_dict, work_dir, processing_counter,
                 entities_number, logging_range_count=10000, ways_batch_size=5000):

        OsmHandler.__init__(self, processing_counter, logging_range_count)
        self.osm_indexer = osm_indexer

        self.geo_json_factory = osmium.geom.GeoJSONFactory()
        self.entities_out_files_dict = files_dict
        self.work_dir = work_dir
        self.gdal_handler = gdal_handler.GDALHandler("gdal/run_ogr.sh", "gdal/osmconf.ini", work_dir)

        self.batch_manager = osm_processing.BatchManager(ways_batch_size, entities_number)

    def generate_batch_osm_file_name(self):
        current_index = self.processing_counter[self.current_entity_type]
        return self.batch_manager.generate_batch_osm_file_name(self.work_dir, self.current_entity_type, current_index)

    def sort_and_write_to_osm_file(self, osm_file_name):
        writer = osmium.SimpleWriter(osm_file_name)

        nodes, ways, relations = self.batch_manager.get_batches_values_sorted_lists()
        for dependency_way in ways:
            writer.add_way(osm_obj_transformer.get_osm_way_from_dict(dependency_way))
        for dependency_node in nodes:
            writer.add_node(osm_obj_transformer.get_osm_node_from_dict(dependency_node))
        for dependency_relation in relations:
            try:
                writer.add_relation(osm_obj_transformer.get_osm_relation_from_dict(dependency_relation))
            except Exception as e:
                logging.info(dependency_relation)
                raise e
        writer.close()

    def write_out_to_jsonl(self, entity_type, entity_dict):
        self.entities_out_files_dict[entity_type].write(json.dumps(entity_dict) + "\n")

    def node(self, node):
        OsmHandler.node(self, node)
        try:
            node_geometry = self.geo_json_factory.create_point(node)
        except osmium._osmium.InvalidLocationError:
            node_geometry = None
        node_dict = osm_obj_transformer.osm_entity_node_dict(node, node_geometry)
        node_dict = osm_obj_transformer.edit_node_dict_according_to_bq_schema(node_dict)
        self.write_out_to_jsonl(self.current_entity_type, node_dict)

    def way(self, way):
        OsmHandler.way(self, way)
        way_dict, way_nodes_dicts = self.get_way_and_its_dependencies_as_dict(way)
        self.batch_manager.replace_ids_in_way_and_its_dependencies(way_dict, way_nodes_dicts)
        self.batch_manager.add_osm_dicts_to_batches(way_nodes_dicts, [way_dict])

        if self.batch_manager.is_full(self.current_entity_type, self.processing_counter):
            temp_osm_file_name = self.generate_batch_osm_file_name()
            self.sort_and_write_to_osm_file(temp_osm_file_name)

            target_ids = self.batch_manager.get_ways_simplified_ids()
            id_geometry_map = self.gdal_handler.osm_to_geojson(temp_osm_file_name, self.current_entity_type, target_ids)

            def add_geometry_and_write(restored_way_dict):
                restored_way_dict = osm_obj_transformer.edit_way_dict_according_to_bq_schema(restored_way_dict)
                self.write_out_to_jsonl(self.current_entity_type, restored_way_dict)

            self.batch_manager.restore_ways_ids_and_add_geometry(id_geometry_map, add_geometry_and_write)

            self.batch_manager.reset()

    def get_way_and_its_dependencies_as_dict(self, way):
        way_dict = osm_obj_transformer.osm_entity_way_dict(way)
        way_timestamp = int(datetime.timestamp(way.timestamp))

        way_nodes = []
        for node in way.nodes:
            try:
                id = node.ref
            except Exception as e:
                id = node
            self.append_node_dict_by_id_and_timestamp(id, way_timestamp, nodes_list=way_nodes)
        return way_dict, way_nodes

    def append_node_dict_by_id_and_timestamp(self, node_id, timestamp, nodes_list=None, nodes_map=None):
        node_dict = self.osm_indexer.get_node_from_index_by_timestamp(node_id, timestamp)
        if node_dict and osm_obj_transformer.is_node_dict_with_location(node_dict):
            if nodes_list is not None:
                nodes_list.append(node_dict)
            elif nodes_map is not None:
                nodes_map[node_id] = node_dict

    def relation(self, relation):
        OsmHandler.relation(self, relation)
        relation_dict, relation_nodes, relation_ways, relation_relations = \
            self.get_relation_and_its_dependencies_as_dict(relation)
        self.batch_manager.replace_ids_in_relation_and_its_dependencies(relation_dict, relation_nodes,
                                                                        relation_ways, relation_relations)
        self.batch_manager.add_osm_dicts_to_batches(relation_nodes, relation_ways, relation_relations, relation_dict)

        if self.batch_manager.is_full(self.current_entity_type, self.processing_counter):
            temp_osm_file_name = self.generate_batch_osm_file_name()
            self.sort_and_write_to_osm_file(temp_osm_file_name)

            target_ids = self.batch_manager.get_main_relations_simplified_ids()
            id_geometry_map = self.gdal_handler.osm_to_geojson(temp_osm_file_name, self.current_entity_type, target_ids)

            def prepare_and_write_out(restored_relation_dict):
                restored_relation_dict = osm_obj_transformer.edit_relation_dict_according_to_bq_schema(
                    restored_relation_dict)
                self.write_out_to_jsonl(self.current_entity_type, restored_relation_dict)

            self.batch_manager.restore_relations_ids_and_add_geometry(id_geometry_map, prepare_and_write_out)

            self.batch_manager.reset()

    def get_relation_and_its_dependencies_as_dict(self, relation):
        relation_dict = osm_obj_transformer.osm_entity_relation_dict(relation)
        relation_timestamp = int(datetime.timestamp(relation.timestamp))

        relation_nodes_map = {}
        relation_ways_map = {}
        relation_relations_map = {}

        for member in iter(relation.members):
            member_id, member_type = member.ref, member.type
            self.append_relation_dependency_objects_dicts(member_id, member_type, relation_timestamp,
                                                          relation_nodes_map, relation_ways_map, relation_relations_map)

        return relation_dict, list(relation_nodes_map.values()), list(relation_ways_map.values()),\
               list(relation_relations_map.values())

    def append_relation_dependency_objects_dicts(self, member_id, member_type, relation_timestamp,
                                                 relation_nodes_map, relation_ways_map, relation_relations_map):
        if member_type == "n":
            if member_id not in relation_nodes_map:
                self.append_node_dict_by_id_and_timestamp(member_id, relation_timestamp, nodes_map=relation_nodes_map)
        elif member_type == "w":
            if member_id not in relation_ways_map:
                way_dict = self.osm_indexer.get_way_from_index_by_timestamp(member_id, relation_timestamp)
                if way_dict:
                    relation_ways_map[member_id] = way_dict
                    for way_node_id in osm_obj_transformer.get_way_nodes(way_dict):
                        self.append_node_dict_by_id_and_timestamp(way_node_id, relation_timestamp,
                                                                  nodes_map=relation_nodes_map)
        elif member_type == "r":
            if member_id not in relation_relations_map:
                relation_dict = self.osm_indexer.get_relation_from_index_by_timestamp(member_id, relation_timestamp)
                if relation_dict:
                    relation_relations_map[member_id] = relation_dict
                    for member_type, member_id, _ in osm_obj_transformer.get_relation_members(relation_dict):
                        self.append_relation_dependency_objects_dicts(member_id, member_type, relation_timestamp,
                                                                      relation_nodes_map, relation_ways_map,
                                                                      relation_relations_map)


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


def make_dir_for_file_if_not_exists(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


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

def file_name_from_path(file_path):
    if "/" in file_path:
        return file_path.split("/")[-1]
    else:
        return file_path

def file_name_without_ext(file_name):
    if "." in file_name:
        return file_name.split(".")[0]
    else:
        return file_name


def create_osm_index(dest_local_file_path, pool_size, pool_index):
    index_db_file_path = file_name_without_ext(dest_local_file_path) + "_{}.sqlite.db".format(pool_index)
    osm_indexer = osm_index.SQLiteOsmIndex(index_db_file_path)
    osm_indexer.create()

    indexing_processing_counter = {key: 0 for key in entities}
    simple_handler = IndexCreator(osm_indexer, indexing_processing_counter,
                                  pool_size=pool_size, pool_index=pool_index)
    simple_handler.apply_file(dest_local_file_path)
    osm_indexer.close()
    return index_db_file_path, indexing_processing_counter


def run_create_osm_index_in_parallel(dest_local_file_path, pool_size):
    pool = multiprocessing.Pool(pool_size)
    results = []
    for pool_index in range(pool_size):
        result = pool.apply_async(create_osm_index, (dest_local_file_path, pool_size, pool_index),
                                  error_callback=lambda err: logging.info("Error: {}".format(err)))
        results.append(result)
    pool.close()
    pool.join()

    return [result.get() for result in results]


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("src_pbf_file_uri", help="The source PBF file to be converted")
    parser.add_argument("dest_gcs_dir", help="URI of GCS dir to save result files")
    parser.add_argument("--num_threads", help="Number of parallel threads for processing", type=int, default=3)

    args = parser.parse_args()

    src_bucket, src_name = parse_uri_to_bucket_and_filename(args.src_pbf_file_uri)

    data_dir = os.environ['DATA_DIR']
    src_file_name = file_name_from_path(src_name)
    dest_local_file_path = data_dir + src_file_name
    make_dir_for_file_if_not_exists(dest_local_file_path)
    from_gcs_to_local_file(src_bucket, src_name, dest_local_file_path)

    entities = ["nodes", "ways", "relations"]

    dbs_and_counters = run_create_osm_index_in_parallel(dest_local_file_path, args.num_threads)
    if len(dbs_and_counters) > 0:
        dbs = [db for db, counter in dbs_and_counters]
        indexing_processing_counter = dbs_and_counters[0][1]

        logging.info(dbs)
        merged_index_db_file_path = file_name_without_ext(dest_local_file_path) + ".sqlite.db"
        osm_index.merge_dbs(merged_index_db_file_path, dbs)

        dest_bucket, dest_dir_name = parse_uri_to_bucket_and_filename(args.dest_gcs_dir)
        upload_file_to_gcs(merged_index_db_file_path, dest_bucket, dest_dir_name+file_name_from_path(merged_index_db_file_path))

        entities_out_files_dict = {}
        results_local_paths = []
        for entity in entities:
            path = data_dir + "{}.jsonl".format(entity)
            results_local_paths.append(path)

            make_dir_for_file_if_not_exists(path)
            entities_out_files_dict[entity] = open(path, "w")

        logging.info("Creating {} files".format(str(results_local_paths)))
        logging.info(indexing_processing_counter)

        converter_processing_counter = {key: 0 for key in entities}
        index_db = osm_index.SQLiteOsmIndex(merged_index_db_file_path)
        history_handler = HistoryHandler(index_db, entities_out_files_dict, data_dir,
                                         converter_processing_counter, indexing_processing_counter)
        history_handler.apply_file(dest_local_file_path)

        index_db.close()

        for entity, out_file in entities_out_files_dict.items():
            out_file.close()

        for path in results_local_paths:
            dest_file_gcs_name = dest_dir_name + file_name_from_path(path)
            upload_file_to_gcs(path, dest_bucket, dest_file_gcs_name)
