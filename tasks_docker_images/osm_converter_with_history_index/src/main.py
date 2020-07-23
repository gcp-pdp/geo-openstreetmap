import osmium
import logging
import os
import errno
import json
import argparse
import multiprocessing

from datetime import datetime
from google.cloud import storage

import elements_transformer
import osm_index
import elements_processing

from gdal import gdal_handler
from parser import OsmParser

OSM_ENTITIES = ["nodes", "ways", "relations"]


class IndexCreator(OsmParser):

    def __init__(self,
                 osm_indexer,
                 processing_counter,
                 last_max_element_timestamp,
                 pool_size=1,
                 pool_index=0,
                 batch_size_to_commit=1000000,
                 logging_range_count=1000000,
                 with_relations=False):
        OsmParser.__init__(self, processing_counter, logging_range_count, pool_size, pool_index)
        self.osm_indexer = osm_indexer
        self.batch_size_to_commit = batch_size_to_commit
        self.with_relations = with_relations
        self.last_max_element_timestamp = last_max_element_timestamp
        self.max_timestamp = 0
        self.added_records = 0

    def get_max_timestamp(self):
        return self.max_timestamp if self.max_timestamp > self.last_max_element_timestamp \
            else self.last_max_element_timestamp

    def node(self, node):
        OsmParser.node(self, node)
        self.process_osm_object(node, self.current_entity_type)

    def way(self, way):
        OsmParser.way(self, way)
        self.process_osm_object(way, self.current_entity_type)

    def relation(self, relation):
        OsmParser.relation(self, relation)
        self.process_osm_object(relation, self.current_entity_type)

    def process_osm_object(self, osm_object, osm_entity_type):
        batch_index = elements_processing.get_uniformly_shard_index_from_id(osm_object.id, self.pool_size)
        if batch_index == self.pool_index:
            osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(osm_object)
            if osm_timestamp > self.last_max_element_timestamp:
                if osm_entity_type == "nodes":
                    self.osm_indexer.add_node_to_index(
                        elements_transformer.osm_entity_node_dict(osm_object,
                                                                  is_simplified=True, osm_timestamp=osm_timestamp))
                elif osm_entity_type == "ways":
                    self.osm_indexer.add_way_to_index(
                        elements_transformer.osm_entity_way_dict(osm_object,
                                                                 is_simplified=True, osm_timestamp=osm_timestamp))
                elif osm_entity_type == "relations":
                    if self.with_relations:
                        self.osm_indexer.add_relation_to_index(
                            elements_transformer.osm_entity_relation_dict(osm_object,
                                                                          is_simplified=True,
                                                                          osm_timestamp=osm_timestamp))
                if osm_timestamp > self.max_timestamp:
                    self.max_timestamp = osm_timestamp
                self.added_records += 1
        self.commit_if_needed()

    def commit_if_needed(self):
        if self.processing_counter[self.current_entity_type] % self.batch_size_to_commit == 0:
            self.osm_indexer.save()
            logging.info("Commit changes to {}. Added records {}".format(self.osm_indexer.get_db_file_path(),
                                                                         self.added_records))
            self.added_records = 0


class HistoryHandler(OsmParser):

    def __init__(self, osm_indexer_map,
                 last_max_element_timestamp, num_shards, files_dict, work_dir, processing_counter, entities_number,
                 pool_index, pool_size, logging_range_count=100000, gdal_batch_size=4, ignore_subrelations=True):

        OsmParser.__init__(self, processing_counter, logging_range_count, pool_size, pool_index)
        self.osm_indexer_map = osm_indexer_map
        self.num_shards = num_shards

        self.geo_json_factory = osmium.geom.GeoJSONFactory()
        self.entities_out_files_dict = files_dict
        self.work_dir = work_dir
        self.gdal_handler = gdal_handler.GDALHandler("gdal/run_ogr.sh", "gdal/osmconf.ini", work_dir)
        self.batch_manager = elements_processing.BatchManager(gdal_batch_size, entities_number)
        self.entities_number = entities_number

        self.ignore_subrelations = ignore_subrelations
        self.last_max_element_timestamp = last_max_element_timestamp

        self.with_single_indexer = len(self.osm_indexer_map) == 1
        self.single_indexer = self.osm_indexer_map[0] if self.with_single_indexer else None

    def get_osm_indexer_by_id(self, id):
        if self.with_single_indexer:
            return self.single_indexer
        else:
            return self.osm_indexer_map[elements_processing.get_uniformly_shard_index_from_id(id, self.num_shards)]

    def generate_batch_osm_file_name(self, pool_size):
        current_index = self.processing_counter[self.current_entity_type]
        return self.batch_manager.generate_batch_osm_file_name(self.work_dir, self.current_entity_type,
                                                               current_index, pool_size)

    def sort_and_write_to_osm_file(self, osm_file_name):
        writer = osmium.SimpleWriter(osm_file_name)

        nodes, ways, relations = self.batch_manager.get_batches_values_sorted_lists()
        for dependency_way in ways:
            writer.add_way(elements_transformer.get_osm_way_from_dict(dependency_way))
        for dependency_node in nodes:
            writer.add_node(elements_transformer.get_osm_node_from_dict(dependency_node))
        for dependency_relation in relations:
            try:
                writer.add_relation(elements_transformer.get_osm_relation_from_dict(dependency_relation))
            except Exception as e:
                logging.info(dependency_relation)
                raise e
        writer.close()

    def write_out_to_jsonl(self, entity_type, entity_dict):
        self.entities_out_files_dict[entity_type].write(json.dumps(entity_dict) + "\n")

    def is_last_element(self):
        return self.processing_counter[self.current_entity_type] == self.entities_number[self.current_entity_type]

    def node(self, node):
        OsmParser.node(self, node)
        if self.is_item_index_for_current_pool_index():
            osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(node)
            if osm_timestamp > self.last_max_element_timestamp:
                node_geometry = str({"type": "Point",
                                     "coordinates": [node.location.lon,
                                                     node.location.lat]}) if node.location.valid() else None
                node_dict = elements_transformer.osm_entity_node_dict(node, node_geometry)
                self.write_out_to_jsonl(self.current_entity_type, node_dict)

    def way(self, way):
        OsmParser.way(self, way)
        if self.is_item_index_for_current_pool_index():
            way_osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(way)
            if way_osm_timestamp > self.last_max_element_timestamp:
                way_dict, way_nodes_dicts = self.get_way_and_its_dependencies_as_dict(way, way_osm_timestamp)
                self.batch_manager.replace_ids_in_way_and_its_dependencies(way_dict, way_nodes_dicts)
                self.batch_manager.add_osm_dicts_to_batches(way_nodes_dicts, [way_dict])

        if self.batch_manager.is_full(self.current_entity_type, self.processing_counter):
            temp_osm_file_name = self.generate_batch_osm_file_name(self.pool_size)
            self.sort_and_write_to_osm_file(temp_osm_file_name)

            target_ids = self.batch_manager.get_ways_simplified_ids()
            id_geometry_map = self.gdal_handler.osm_to_geojson(temp_osm_file_name, self.current_entity_type,
                                                               target_ids)

            def add_geometry_and_write(restored_way_dict):
                restored_way_dict = elements_transformer.edit_way_dict_according_to_bq_schema(restored_way_dict)
                self.write_out_to_jsonl(self.current_entity_type, restored_way_dict)

            self.batch_manager.restore_ways_ids_and_add_geometry(id_geometry_map, add_geometry_and_write)
            self.batch_manager.reset()

    def relation(self, relation):
        OsmParser.relation(self, relation)
        if self.is_item_index_for_current_pool_index() or self.is_last_element():
            osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(relation)
            if osm_timestamp > self.last_max_element_timestamp:
                relation_dict, relation_nodes, relation_ways, relation_relations = \
                    self.get_relation_and_its_dependencies_as_dict(relation)
                self.batch_manager.replace_ids_in_relation_and_its_dependencies(relation_dict, relation_nodes,
                                                                                relation_ways, relation_relations)

                self.batch_manager.add_osm_dicts_to_batches(relation_nodes, relation_ways, relation_relations,
                                                            relation_dict)

        if self.batch_manager.is_full(self.current_entity_type, self.processing_counter):
            temp_osm_file_name = self.generate_batch_osm_file_name(self.pool_size)
            self.sort_and_write_to_osm_file(temp_osm_file_name)

            target_ids = self.batch_manager.get_main_relations_simplified_ids()
            id_geometry_map = self.gdal_handler.osm_to_geojson(temp_osm_file_name, self.current_entity_type,
                                                               target_ids)

            def prepare_and_write_out(restored_relation_dict):
                restored_relation_dict = elements_transformer.edit_relation_dict_according_to_bq_schema(
                    restored_relation_dict)
                self.write_out_to_jsonl(self.current_entity_type, restored_relation_dict)

            self.batch_manager.restore_relations_ids_and_add_geometry(id_geometry_map, prepare_and_write_out)
            self.batch_manager.reset()

    def get_way_and_its_dependencies_as_dict(self, way, way_osm_timestamp):
        way_dict = elements_transformer.osm_entity_way_dict(way, tags_to_bq=False, osm_timestamp=way_osm_timestamp)
        way_nodes = []
        for node in way.nodes:
            self.append_node_dict_by_id_and_timestamp(node.ref, way_osm_timestamp, nodes_list=way_nodes)
        return way_dict, way_nodes

    def append_node_dict_by_id_and_timestamp(self, node_id, timestamp, nodes_list=None, nodes_map=None):
        indexer = self.get_osm_indexer_by_id(node_id)
        node_dict = indexer.get_node_from_index_by_timestamp(node_id, timestamp)
        if node_dict and elements_transformer.is_node_dict_with_location(node_dict):
            if nodes_list is not None:
                nodes_list.append(node_dict)
            elif nodes_map is not None:
                nodes_map[node_id] = node_dict

    def get_relation_and_its_dependencies_as_dict(self, relation):
        relation_dict = elements_transformer.osm_entity_relation_dict(relation, tags_to_bq=False)
        relation_timestamp = int(datetime.timestamp(relation.timestamp))

        relation_nodes_map = {}
        relation_ways_map = {}
        relation_relations_map = {}

        for member in iter(relation.members):
            member_id, member_type = member.ref, member.type
            self.append_relation_dependency_objects_dicts(member_id, member_type, relation_timestamp,
                                                          relation_nodes_map, relation_ways_map, relation_relations_map)

        return relation_dict, list(relation_nodes_map.values()), list(relation_ways_map.values()), \
               list(relation_relations_map.values())

    def append_relation_dependency_objects_dicts(self, member_id, member_type, relation_timestamp,
                                                 relation_nodes_map, relation_ways_map, relation_relations_map):
        if member_type == "n":
            if member_id not in relation_nodes_map:
                self.append_node_dict_by_id_and_timestamp(member_id, relation_timestamp, nodes_map=relation_nodes_map)
        elif member_type == "w":
            if member_id not in relation_ways_map:
                current_indexer = self.get_osm_indexer_by_id(member_id)
                way_dict = current_indexer.get_way_from_index_by_timestamp(member_id, relation_timestamp)
                if way_dict:
                    relation_ways_map[member_id] = way_dict
                    for way_node_id in elements_transformer.get_way_nodes(way_dict):
                        self.append_node_dict_by_id_and_timestamp(way_node_id, relation_timestamp,
                                                                  nodes_map=relation_nodes_map)
        elif member_type == "r":
            if not self.ignore_subrelations:
                if member_id not in relation_relations_map:
                    relation_dict = self.get_osm_indexer_by_id(member_id) \
                        .get_relation_from_index_by_timestamp(member_id, relation_timestamp)
                    if relation_dict:
                        relation_relations_map[member_id] = relation_dict
                        for member_type, member_id, _ in elements_transformer.get_relation_members(relation_dict):
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


def create_processing_counter():
    return {key: 0 for key in OSM_ENTITIES}


def run_index_creator(dest_local_file_path, osm_indexer, indexing_processing_counter,
                      num_shards, pool_index, last_max_element_timestamp):
    index_creator = IndexCreator(osm_indexer, indexing_processing_counter, last_max_element_timestamp,
                                 pool_size=num_shards, pool_index=pool_index)
    index_creator.apply_file(dest_local_file_path)
    return index_creator.get_max_timestamp()


def create_osm_index(osm_local_file_path, db_file_path, num_shards, pool_index,
                     last_max_element_timestamp):
    db_exists = os.path.exists(db_file_path)
    osm_indexer = osm_index.SQLiteOsmIndex(db_file_path)
    if not db_exists:
        osm_indexer.create()
        logging.info("Creating DB {} ".format(db_file_path))
    indexing_processing_counter = create_processing_counter()
    max_timestamp = run_index_creator(osm_local_file_path, osm_indexer, indexing_processing_counter,
                                      num_shards, pool_index,
                                      last_max_element_timestamp)
    osm_indexer.close()
    return indexing_processing_counter, max_timestamp


def run_create_osm_index_in_parallel(dest_local_file_path, dbs_file_paths, last_max_element_timestamp):
    pool = multiprocessing.Pool(num_threads)
    results = []
    for db_file_path in dbs_file_paths:
        pool_index = dbs_file_paths.index(db_file_path)
        result = pool.apply_async(create_osm_index, (dest_local_file_path, db_file_path, num_threads, pool_index,
                                                     last_max_element_timestamp),
                                  error_callback=lambda err: logging.info("Error: {}".format(err)))
        results.append(result)
    pool.close()
    pool.join()

    counters_max_timestamps = [result.get() for result in results]

    max_timestamps = [max_timestamp for _, max_timestamp in counters_max_timestamps]
    indexing_processing_counter = counters_max_timestamps[0][0]

    return indexing_processing_counter, max(max_timestamps)


def merge_if_db_needed_and_upload_to_gcs(dbs, index_db_file_path, merge_db_shards, db_exists):
    if merge_db_shards:
        dbs_to_merge = list(dbs.values())
        osm_index.merge_dbs(index_db_file_path, dbs_to_merge, db_exists)
        upload_file_to_gcs(index_db_file_path, converted_results_bucket,
                           converted_results_gcs_dir + file_name_from_path(index_db_file_path))
        return {0: index_db_file_path}
    else:
        for shard_index, db_file_path in dbs.items():
            upload_file_to_gcs(db_file_path, converted_results_bucket,
                               converted_results_gcs_dir + file_name_from_path(db_file_path))
        return dbs


def create_output_files(index):
    entities_out_files_dict = {}
    results_local_paths = []
    for entity in OSM_ENTITIES:
        path = data_dir + "{}_{}.jsonl".format(entity, index)
        results_local_paths.append(path)

        make_dir_for_file_if_not_exists(path)
        entities_out_files_dict[entity] = open(path, "w")

    logging.info("Creating {} files".format(str(results_local_paths)))
    return entities_out_files_dict, results_local_paths


def get_metadata_file_path(db_name):
    return file_name_without_ext(db_name) + ".metadata.txt"


def metadata_from_file(file_path):
    try:
        with open(file_path, "r") as f:
            metadata_json = json.load(f)
        return metadata_json["counter"], int(metadata_json["db"]), int(metadata_json["history"])
    except Exception:
        return create_processing_counter(), 0, 0


def metadata_to_file(file_path, processing_counter, max_timestamp_db, max_timestamp_history):
    with open(file_path, "w") as f:
        json.dump({"counter": processing_counter, "db": max_timestamp_db, "history": max_timestamp_history}, f)
    return file_path


def download_db_and_last_max_element_timestamp_if_exists(dbs_file_paths,
                                                         timestamps_file_path,
                                                         dest_bucket, dest_dir_name):
    db_gcs_and_local_paths = []
    for db_file_path in dbs_file_paths:
        db_name = file_name_from_path(db_file_path)
        db_blob_name = dest_dir_name + db_name

        if not is_gcs_blob_exists(dest_bucket, db_blob_name):
            return create_processing_counter(), 0, 0
        else:
            db_gcs_and_local_paths.append((db_blob_name, db_file_path))

    for db_blob_name, db_file_path in db_gcs_and_local_paths:
        from_gcs_to_local_file(dest_bucket, db_blob_name, db_file_path)

    timestamps_file_name = file_name_from_path(timestamps_file_path)
    timestamps_file_blob_name = dest_dir_name + timestamps_file_name
    if is_gcs_blob_exists(dest_bucket, timestamps_file_blob_name):
        from_gcs_to_local_file(dest_bucket, timestamps_file_blob_name, timestamps_file_path)
        return metadata_from_file(timestamps_file_path)
    else:
        return create_processing_counter(), 0, 0


def upload_metadata_to_gcs(timestamps_file_path, indexing_processing_counter,
                           max_timestamp_db, max_timestamp_history, dest_bucket, dest_dir_name):
    metadata_to_file(timestamps_file_path, indexing_processing_counter, max_timestamp_db, max_timestamp_history)

    timestamps_file_name = file_name_from_path(timestamps_file_path)
    timestamps_file_blob_name = dest_dir_name + timestamps_file_name
    upload_file_to_gcs(timestamps_file_path, dest_bucket, timestamps_file_blob_name)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("src_pbf_file_uri", help="The source PBF file to be converted")
    parser.add_argument("--converted_gcs_dir", help="URI of GCS dir to save result files", required=True)
    parser.add_argument("--index_db_and_metadata_gcs_dir", help="URI of GCS dir with index DB and metadata files",
                        required=True)
    parser.add_argument("--num_db_shards", help="Number of db to create in indexing mode", type=int, default=2)
    parser.add_argument("--overwrite", help="Overwrite previous OSM index database and results",
                        type=bool, default=False)
    parser.add_argument("--create_index_mode", help="Run in index creation mode", dest='create_index_mode',
                        action='store_true')
    parser.add_argument("--history_processing_pool_index", type=int, default=0)
    parser.add_argument("--history_processing_pool_size", type=int, default=1)

    args = parser.parse_args()
    num_threads = args.num_db_shards
    src_bucket, src_name = parse_uri_to_bucket_and_filename(args.src_pbf_file_uri)
    index_db_and_metadata_bucket, index_db_and_metadata_gcs_dir = \
        parse_uri_to_bucket_and_filename(args.index_db_and_metadata_gcs_dir)
    converted_results_bucket, converted_results_gcs_dir = parse_uri_to_bucket_and_filename(args.converted_gcs_dir)

    data_dir = os.environ['DATA_DIR']
    src_file_name = file_name_from_path(src_name)
    osm_local_file_path = data_dir + src_file_name
    make_dir_for_file_if_not_exists(osm_local_file_path)
    from_gcs_to_local_file(src_bucket, src_name, osm_local_file_path)

    index_db_file_path_pattern = file_name_without_ext(osm_local_file_path) + "_{}_{}.sqlite.db"
    metadata_file_path = get_metadata_file_path(osm_local_file_path)
    dbs_file_paths = [index_db_file_path_pattern.format(index, num_threads) for index in range(1, num_threads + 1)]

    if args.overwrite and args.create_index_mode:
        processing_counter, max_timestamp_db, max_timestamp_history = create_processing_counter(), 0, 0
    else:
        processing_counter, max_timestamp_db, max_timestamp_history = \
            download_db_and_last_max_element_timestamp_if_exists(
                dbs_file_paths, metadata_file_path, index_db_and_metadata_bucket, index_db_and_metadata_gcs_dir)

    if args.create_index_mode:
        indexing_processing_counter, max_timestamp = \
            run_create_osm_index_in_parallel(osm_local_file_path, dbs_file_paths, max_timestamp_db)
        max_timestamp_db = max_timestamp
        logging.info("Indexing processing counter: {}".format(indexing_processing_counter))
        logging.info("Max OSM object timestamp: {}".format(max_timestamp))

        for db_file_path in dbs_file_paths:
            db_gcs_name = index_db_and_metadata_gcs_dir + file_name_from_path(db_file_path)
            upload_file_to_gcs(db_file_path, index_db_and_metadata_bucket, db_gcs_name)
        upload_metadata_to_gcs(metadata_file_path, indexing_processing_counter, max_timestamp_db, max_timestamp_history,
                               index_db_and_metadata_bucket,
                               index_db_and_metadata_gcs_dir)
    else:
        pool_index = args.history_processing_pool_index
        pool_size = args.history_processing_pool_size

        entities_out_files_dict, results_local_paths = create_output_files(args.history_processing_pool_index)

        osm_indexer_map = {shard_index: osm_index.SQLiteOsmIndex(db_file_path)
                           for shard_index, db_file_path in enumerate(dbs_file_paths)}
        history_handler = HistoryHandler(osm_indexer_map, max_timestamp_history,
                                         args.num_db_shards, entities_out_files_dict, data_dir,
                                         create_processing_counter(), processing_counter,
                                         pool_index, pool_size)
        history_handler.apply_file(osm_local_file_path)

        for shard_index, osm_indexer in osm_indexer_map.items():
            osm_indexer.close()
        for entity, out_file in entities_out_files_dict.items():
            out_file.close()

        max_timestamp_history = max_timestamp_db
        for path in results_local_paths:
            dest_file_gcs_name = converted_results_gcs_dir + file_name_from_path(path)
            upload_file_to_gcs(path, converted_results_bucket, dest_file_gcs_name)
        upload_metadata_to_gcs(metadata_file_path, processing_counter, max_timestamp_db, max_timestamp_history,
                               index_db_and_metadata_bucket, index_db_and_metadata_gcs_dir)
