import osmium
import logging
import time

import elements_transformer
import elements_processing

from xml.sax import handler
import psutil


def to_mb(bytes_num):
    return int(bytes_num / (1024 * 1024))


class OsmParser(osmium.SimpleHandler):

    def __init__(self, processing_counter, logging_range_count, pool_size=1, pool_index=0):
        osmium.SimpleHandler.__init__(self)

        self.processing_counter = processing_counter
        self.last_log_time = time.time()
        self.logging_range_count = logging_range_count
        self.current_entity_type = ""
        self.pool_index = pool_index
        self.pool_size = pool_size

    def current_pool_index(self):
        return self.processing_counter[self.current_entity_type] % self.pool_size

    def is_item_index_for_current_pool_index(self):
        return self.current_pool_index() == self.pool_index

    def log_processing(self):
        self.processing_counter[self.current_entity_type] = self.processing_counter[self.current_entity_type] + 1
        if self.processing_counter[self.current_entity_type] % self.logging_range_count == 0:
            virtual_memory = psutil.virtual_memory()
            logging.info(self.current_entity_type + " ({}/{}) ".format(self.pool_index + 1, self.pool_size)
                         + str(self.processing_counter[self.current_entity_type])
                         + " " + str(time.time() - self.last_log_time)
                         + " Memory: usage {}, free {} MB, used {} MB"
                         .format(virtual_memory.percent, to_mb(virtual_memory.free), to_mb(virtual_memory.used)))
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


class IndexCreatorWithXmlParser(handler.ContentHandler):

    def __init__(self, osm_indexer_map,
                 processing_counter, num_shards,
                 is_id_hash_partitioned_shards,
                 pool_size=1, pool_index=0,
                 batch_size_to_commit=1000000,
                 logging_range_count=1000000):
        handler.ContentHandler.__init__(self)
        self.processing_counter = processing_counter
        self.last_log_time = time.time()
        self.logging_range_count = logging_range_count
        self.current_entity_type = ""
        self.pool_index = pool_index
        self.pool_size = pool_size

        self.xml_hierarchy = []
        self.current_obj = {}

        self.xml_entity_map = {"node": "nodes", "way": "ways", "relation": "relations"}

        self.osm_indexer_map = osm_indexer_map
        self.num_shards = num_shards
        self.batch_size_to_commit = batch_size_to_commit
        self.is_id_hash_partitioned_shards = is_id_hash_partitioned_shards

        self.current_indexer = None

    def log_processing(self):
        self.processing_counter[self.current_entity_type] = self.processing_counter[self.current_entity_type] + 1
        if self.processing_counter[self.current_entity_type] % self.logging_range_count == 0:
            logging.info(self.current_entity_type + " ({}/{}) ".format(self.pool_index + 1, self.pool_size)
                         + str(self.processing_counter[self.current_entity_type])
                         + " " + str(time.time() - self.last_log_time))
            self.last_log_time = time.time()

    def startDocument(self):
        pass

    def get_current_xml_hierarchy_level(self):
        return self.xml_hierarchy[len(self.xml_hierarchy) - 1]

    def process_element(self, name, attributes):
        if name == "node":
            self.current_obj = elements_transformer.osm_entity_node_dict(attributes,
                                                                         is_simplified=True,
                                                                         is_xml_attributes=True)

    def startElement(self, name, attributes):
        self.xml_hierarchy.append(name)

        if name in self.xml_entity_map:
            self.current_entity_type = self.xml_entity_map[name]
            self.log_processing()

            if not self.is_id_hash_partitioned_shards:
                batch_index = self.processing_counter[self.current_entity_type] % self.num_shards
            else:
                obj_id = attributes["id"]
                batch_index = elements_processing.get_uniformly_shard_index_from_id(obj_id, self.num_shards)
            if batch_index in self.osm_indexer_map:
                self.process_element(name, attributes)
                self.current_indexer = self.osm_indexer_map[batch_index]
            else:
                self.current_indexer = None

    def endElement(self, name, *args):
        if name == "node" and self.current_indexer:
            self.on_node_element(self.current_obj)
            self.current_indexer = None
        del self.xml_hierarchy[-1]

    def characters(self, data):
        pass

    def on_node_element(self, node_dict):
        pass

    def on_way_element(self, way_dict):
        pass

    def on_relation_element(self, relation_dict):
        pass
