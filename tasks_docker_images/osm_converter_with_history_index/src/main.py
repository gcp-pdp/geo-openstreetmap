import osmium
import logging
import os
import json
import argparse
import multiprocessing
import datetime
import time

import elements_transformer
import osm_index
import elements_processing
import cache_manager
import gcs_service
import file_service

from gdal import gdal_handler
from parser import OsmParser

OSM_ENTITIES = ["nodes", "ways", "relations"]


class IndexCreator(OsmParser):

    def __init__(self,
                 osm_indexer_map,
                 processing_counter,
                 last_max_element_timestamp,
                 num_db_shards,
                 pool_size=1,
                 pool_index=0,
                 batch_size_to_commit=1000000,
                 logging_range_count=1000000,
                 with_relations=False):
        OsmParser.__init__(self, processing_counter, logging_range_count, pool_size, pool_index)
        self.osm_indexer_map = osm_indexer_map
        self.batch_size_to_commit = batch_size_to_commit
        self.with_relations = with_relations
        self.last_max_element_timestamp = last_max_element_timestamp
        self.num_db_shards = num_db_shards
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
        batch_index = elements_processing.get_uniformly_shard_index_from_id(osm_object.id, self.num_db_shards)
        if batch_index in self.osm_indexer_map:
            osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(osm_object)
            if osm_timestamp > self.last_max_element_timestamp:
                osm_indexer = self.osm_indexer_map[batch_index]
                if osm_entity_type == "nodes":
                    osm_indexer.add_node_to_index(
                        elements_transformer.osm_entity_node_dict(osm_object,
                                                                  is_simplified=True, osm_timestamp=osm_timestamp))
                elif osm_entity_type == "ways":
                    osm_indexer.add_way_to_index(
                        elements_transformer.osm_entity_way_dict(osm_object,
                                                                 is_simplified=True, osm_timestamp=osm_timestamp))
                elif osm_entity_type == "relations":
                    if self.with_relations:
                        osm_indexer.add_relation_to_index(
                            elements_transformer.osm_entity_relation_dict(osm_object,
                                                                          is_simplified=True,
                                                                          osm_timestamp=osm_timestamp))
                if osm_timestamp > self.max_timestamp:
                    self.max_timestamp = osm_timestamp
                self.added_records += 1
        self.commit_if_needed()

    def commit_if_needed(self):
        if self.processing_counter[self.current_entity_type] % self.batch_size_to_commit == 0:
            for index, osm_indexer in self.osm_indexer_map.items():
                osm_indexer.save()
                logging.info("Commit changes to {}. Added records {}".format(osm_indexer.get_db_file_path(),
                                                                             self.added_records))
            self.added_records = 0


class HistoryHandler(OsmParser):

    def __init__(self, osm_indexer_map,
                 last_max_element_timestamp, num_shards, files_dict, work_dir, processing_counter, entities_number,
                 pool_index, pool_size, logging_range_count=100000, gdal_batch_size=5000, ignore_subrelations=True):

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

    def log_indexer_efficiency_data(self):
        total_queries = 0
        total_time = 0
        for index, value in self.osm_indexer_map.items():
            total_queries += value.get_query_counter()
            total_time += value.get_query_time()
            value.reset_query_counter()
            value.reset_query_time()

        logging.info("Executed queries: {}, time: {}. Approx secs/query: {}".format(total_queries, total_time,
                                                                                    total_time/total_queries))

    def node(self, node):
        OsmParser.node(self, node)
        # TODO
        # if self.is_item_index_for_current_pool_index():
        #     osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(node)
        #     if osm_timestamp > self.last_max_element_timestamp:
        #         node_geometry = str({"type": "Point",
        #                              "coordinates": [node.location.lon,
        #                                              node.location.lat]}) if node.location.valid() else None
        #         node_dict = elements_transformer.osm_entity_node_dict(node, node_geometry)
        #         self.write_out_to_jsonl(self.current_entity_type, node_dict)

    def way(self, way):
        OsmParser.way(self, way)
        # TODO
        # if self.is_item_index_for_current_pool_index():
        #     way_osm_timestamp = elements_transformer.osm_timestamp_from_osm_entity(way)
        #     if way_osm_timestamp > self.last_max_element_timestamp:
        #         way_dict, way_nodes_dicts = self.get_way_and_its_dependencies_as_dict(way, way_osm_timestamp)
        #         self.batch_manager.replace_ids_in_way_and_its_dependencies(way_dict, way_nodes_dicts)
        #         self.batch_manager.add_osm_dicts_to_batches(way_nodes_dicts, [way_dict])
        #
        # if self.batch_manager.is_full(self.current_entity_type, self.processing_counter):
        #     temp_osm_file_name = self.generate_batch_osm_file_name(self.pool_size)
        #     self.sort_and_write_to_osm_file(temp_osm_file_name)
        #
        #     target_ids = self.batch_manager.get_ways_simplified_ids()
        #     id_geometry_map = self.gdal_handler.osm_to_geojson(temp_osm_file_name, self.current_entity_type,
        #                                                        target_ids)
        #
        #     def add_geometry_and_write(restored_way_dict):
        #         restored_way_dict = elements_transformer.edit_way_dict_according_to_bq_schema(restored_way_dict)
        #         self.write_out_to_jsonl(self.current_entity_type, restored_way_dict)
        #
        #     self.batch_manager.restore_ways_ids_and_add_geometry(id_geometry_map, add_geometry_and_write)
        #     self.batch_manager.reset()
        #
        #     self.log_indexer_efficiency_data()

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

            self.log_indexer_efficiency_data()

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
        relation_timestamp = int(datetime.datetime.timestamp(relation.timestamp))

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


def run_index_creator(dest_local_file_path, osm_indexer_map, indexing_processing_counter,
                      num_db_shards,
                      pool_size, pool_index, last_max_element_timestamp):
    index_creator = IndexCreator(osm_indexer_map,
                                 indexing_processing_counter,
                                 last_max_element_timestamp,
                                 num_db_shards,
                                 pool_size=pool_size,
                                 pool_index=pool_index)
    index_creator.apply_file(dest_local_file_path)
    return index_creator.get_max_timestamp()


def create_osm_index(osm_local_file_path, index_and_db_map,
                     num_db_shards,
                     pool_size, pool_index,
                     last_max_element_timestamp):
    osm_indexer_map = {}
    for index, db_file_path in index_and_db_map:
        db_exists = os.path.exists(db_file_path)
        current_osm_indexer = osm_index.SQLiteOsmIndex(db_file_path)
        if not db_exists:
            current_osm_indexer.create()
            logging.info("Creating DB {} ".format(db_file_path))
        osm_indexer_map[index] = current_osm_indexer

    indexing_processing_counter = cache_manager.create_processing_counter(OSM_ENTITIES)
    max_timestamp = run_index_creator(osm_local_file_path, osm_indexer_map, indexing_processing_counter,
                                      num_db_shards,
                                      pool_size, pool_index,
                                      last_max_element_timestamp)

    for index, indexer in osm_indexer_map.items():
        indexer.close()
    return indexing_processing_counter, max_timestamp


def run_create_osm_index_in_parallel(dest_local_file_path, dbs_file_paths, num_threads, last_max_element_timestamp):
    pool = multiprocessing.Pool(num_threads)

    pools_and_dbs = [[] for pool_index in range(num_threads)]
    for index, db_file_path in enumerate(dbs_file_paths):
        pool_index = index % num_threads
        pools_and_dbs[pool_index].append((index, db_file_path))

    results = []
    for pool_index in range(num_threads):
        result = pool.apply_async(create_osm_index, (dest_local_file_path, pools_and_dbs[pool_index],
                                                     len(dbs_file_paths), num_threads, pool_index,
                                                     last_max_element_timestamp),
                                  error_callback=lambda err: logging.info("Error: {}".format(err)))
        results.append(result)
    pool.close()
    pool.join()

    counters_max_timestamps = [result.get() for result in results]

    max_timestamps = [max_timestamp for _, max_timestamp in counters_max_timestamps]
    indexing_processing_counter = counters_max_timestamps[0][0]

    return indexing_processing_counter, max(max_timestamps)


def create_output_files(index):
    entities_out_files_dict = {}
    results_local_paths = []
    for entity in OSM_ENTITIES:
        path = data_dir + "{}_{}.jsonl".format(entity, index)
        results_local_paths.append(path)

        file_service.make_dir_for_file_if_not_exists(path)
        entities_out_files_dict[entity] = open(path, "w")

    logging.info("Creating {} files".format(str(results_local_paths)))
    return entities_out_files_dict, results_local_paths


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("src_pbf_file_uri", help="The source PBF file to be converted")
    parser.add_argument("--converted_gcs_dir", help="URI of GCS dir to save result files", required=True)
    parser.add_argument("--index_db_and_metadata_gcs_dir", help="URI of GCS dir with index DB and metadata files",
                        required=True)
    parser.add_argument("--num_threads", help="Number of threads to create index", type=int, default=2)
    parser.add_argument("--num_db_shards", help="Number of db to create in indexing mode", type=int, default=2)
    parser.add_argument("--overwrite", help="Overwrite previous OSM index database and results",
                        type=bool, default=False)
    parser.add_argument("--create_index_mode", help="Run in index creation mode", dest='create_index_mode',
                        action='store_true')
    parser.add_argument("--history_processing_pool_index", type=int, default=0)
    parser.add_argument("--history_processing_pool_size", type=int, default=1)
    #TODO
    # parser.add_argument("--data_freshness_exp_days", type=int, default=5)
    parser.add_argument("--data_freshness_exp_days", type=int, default=15)

    args = parser.parse_args()
    num_db_shards = args.num_db_shards
    num_threads = args.num_threads

    if num_threads > num_db_shards:
        num_threads = num_db_shards

    src_bucket, src_name = gcs_service.parse_uri_to_bucket_and_filename(args.src_pbf_file_uri)
    index_db_and_metadata_bucket, index_db_and_metadata_gcs_dir = \
        gcs_service.parse_uri_to_bucket_and_filename(args.index_db_and_metadata_gcs_dir)
    converted_results_bucket, converted_results_gcs_dir = \
        gcs_service.parse_uri_to_bucket_and_filename(args.converted_gcs_dir)

    data_dir = os.environ['DATA_DIR']
    src_file_name = file_service.file_name_from_path(src_name)
    osm_local_file_path = data_dir + src_file_name
    file_service.make_dir_for_file_if_not_exists(osm_local_file_path)

    index_db_file_path_pattern = file_service.file_name_without_ext(osm_local_file_path) + "_{}_{}.sqlite.db"
    metadata_file_path = cache_manager.get_metadata_file_path(osm_local_file_path, num_db_shards)
    dbs_file_paths = [index_db_file_path_pattern.format(index, num_db_shards) for index in range(1, num_db_shards + 1)]

    metadata = cache_manager.download_and_read_metadata_file(metadata_file_path, index_db_and_metadata_bucket,
                                                             index_db_and_metadata_gcs_dir)
    logging.info("Metadata fot {}: {}".format(src_name, metadata))
    max_timestamp_db, max_timestamp_history, last_updated_db, last_updated_history, processing_counter = \
        cache_manager.get_values_from_metadata(metadata, OSM_ENTITIES)

    if args.create_index_mode:
        if args.overwrite:
            max_timestamp_db = 0
        if cache_manager.is_file_fresh(last_updated_db, args.data_freshness_exp_days):
            logging.info("Index DB is fresh")
        else:
            gcs_service.from_gcs_to_local_file(src_bucket, src_name, osm_local_file_path)
            if max_timestamp_db > 0:
                cache_manager.download_db_if_exists(dbs_file_paths, index_db_and_metadata_bucket,
                                                    index_db_and_metadata_gcs_dir)
            indexing_processing_counter, max_timestamp = \
                run_create_osm_index_in_parallel(osm_local_file_path, dbs_file_paths, num_threads, max_timestamp_db)
            max_timestamp_db = max_timestamp
            logging.info("Indexing processing counter: {}".format(indexing_processing_counter))
            logging.info("Max OSM object timestamp: {}".format(max_timestamp))

            last_updated_db = int(time.time())
            for db_file_path in dbs_file_paths:
                db_gcs_name = index_db_and_metadata_gcs_dir + file_service.file_name_from_path(db_file_path)
                gcs_service.upload_file_to_gcs(db_file_path, index_db_and_metadata_bucket, db_gcs_name)
            cache_manager.upload_metadata_to_gcs(metadata_file_path, indexing_processing_counter,
                                                 max_timestamp_db, max_timestamp_history,
                                                 last_updated_db, last_updated_history,
                                                 index_db_and_metadata_bucket,
                                                 index_db_and_metadata_gcs_dir)
    else:
        if args.overwrite:
            max_timestamp_history = 0
        if cache_manager.is_file_fresh(last_updated_history, args.data_freshness_exp_days):
            logging.info("JSONL result files are fresh")
        else:
            gcs_service.from_gcs_to_local_file(src_bucket, src_name, osm_local_file_path)
            db_shards_exist = cache_manager.download_db_if_exists(dbs_file_paths, index_db_and_metadata_bucket,
                                                                  index_db_and_metadata_gcs_dir)
            if not db_shards_exist:
                logging.info("Could not find suitable DB shards for current args: {}".format(str(args)))
                exit(1)

            pool_index = args.history_processing_pool_index
            pool_size = args.history_processing_pool_size

            entities_out_files_dict, results_local_paths = create_output_files(args.history_processing_pool_index)

            osm_indexer_map = {shard_index: osm_index.SQLiteOsmIndex(db_file_path)
                               for shard_index, db_file_path in enumerate(dbs_file_paths)}
            history_processing_counter = cache_manager.create_processing_counter(OSM_ENTITIES)
            history_handler = HistoryHandler(osm_indexer_map, max_timestamp_history,
                                             args.num_db_shards, entities_out_files_dict, data_dir,
                                             history_processing_counter, processing_counter,
                                             pool_index, pool_size)
            history_handler.apply_file(osm_local_file_path)

            for shard_index, osm_indexer in osm_indexer_map.items():
                osm_indexer.close()
            for entity, out_file in entities_out_files_dict.items():
                out_file.close()

            max_timestamp_history = max_timestamp_db
            last_updated_history = int(time.time())
            for path in results_local_paths:
                dest_file_gcs_name = converted_results_gcs_dir + file_service.file_name_from_path(path)
                gcs_service.upload_file_to_gcs(path, converted_results_bucket, dest_file_gcs_name)
            cache_manager.upload_metadata_to_gcs(metadata_file_path, processing_counter,
                                                 max_timestamp_db, max_timestamp_history,
                                                 last_updated_db, last_updated_history,
                                                 index_db_and_metadata_bucket, index_db_and_metadata_gcs_dir)
