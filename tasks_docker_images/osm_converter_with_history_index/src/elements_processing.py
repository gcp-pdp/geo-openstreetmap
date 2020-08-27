import json
import hashlib


def generate_complex_id(obj_dict):
    return "{}_{}".format(obj_dict["id"], obj_dict["version"])


def get_uniformly_shard_index_from_id(id, num_shards):
    return int(hashlib.md5(str(id).encode("utf-8")).hexdigest(), 16) % num_shards


class IdManager(object):

    def __init__(self):
        self.relation_id_map = {}
        self.way_id_map = {}
        self.node_id_map = {}

        self.id_counter = 1

    def replace_ids_in_way_and_its_dependencies(self, way_dict, way_nodes_dicts):
        self.replace_ids_in_obj_list([way_dict], self.way_id_map)
        local_id_map = self.replace_ids_in_obj_list(way_nodes_dicts, self.node_id_map)
        way_dict["nodes"] = [local_id_map[node_id] if node_id in local_id_map else node_id
                             for node_id in way_dict["nodes"]]

    def replace_ids_in_relation_and_its_dependencies(self, relation_dict, relation_nodes_dicts,
                                                     relation_ways_dicts, relation_relations_dicts):
        self.replace_ids_in_obj_list([relation_dict], self.relation_id_map)
        nodes_local_id_map = self.replace_ids_in_obj_list(relation_nodes_dicts, self.node_id_map)
        ways_local_id_map = self.replace_ids_in_obj_list(relation_ways_dicts, self.way_id_map)
        relations_local_id_map = self.replace_ids_in_obj_list(relation_relations_dicts, self.relation_id_map)

        for relation_way_dict in relation_ways_dicts:
            self.replace_ids_in_way_nodes(relation_way_dict, nodes_local_id_map)
        for relation_relation_dict in relation_relations_dicts:
            self.replace_ids_in_relation_members(relation_relation_dict, nodes_local_id_map,
                                                 ways_local_id_map, relations_local_id_map)
        self.replace_ids_in_relation_members(relation_dict, nodes_local_id_map,
                                             ways_local_id_map, relations_local_id_map)

    def replace_ids_in_way_nodes(self, way_dict, id_map):
        way_dict["nodes"] = [id_map[node_id] if node_id in id_map else node_id for node_id in way_dict["nodes"]]

    def replace_ids_in_relation_members(self, relation_dict, nodes_id_map, ways_id_map, relations_id_map):
        for index in range(len(relation_dict["members"])):
            member = relation_dict["members"][index]
            member_type, member_id, member_role = member
            if member_type == "n":
                relation_dict["members"][index] = (member_type,
                          nodes_id_map[member_id] if member_id in nodes_id_map else member_id, member_role)
            elif member_type == "w":
                relation_dict["members"][index] = (member_type,
                          ways_id_map[member_id] if member_id in ways_id_map else member_id, member_role)
            elif member_type == "r":
                relation_dict["members"][index] = (member_type,
                          relations_id_map[member_id] if member_id in relations_id_map else member_id, member_role)

    def replace_ids_in_obj_list(self, osm_obj_dicts, id_map):
        local_id_map = {}
        for osm_obj_dict in osm_obj_dicts:
            osm_obj_complex_id = generate_complex_id(osm_obj_dict)
            if osm_obj_complex_id in id_map:
                local_id_map[osm_obj_dict["id"]] = id_map[osm_obj_complex_id]
                osm_obj_dict["id"] = id_map[osm_obj_complex_id]
            else:
                id_map[osm_obj_complex_id] = self.id_counter
                local_id_map[osm_obj_dict["id"]] = self.id_counter
                osm_obj_dict["id"] = self.id_counter
                self.id_counter = self.id_counter + 1
        return local_id_map

    def get_simplified_id_and_original_id_maps(self):
        result_relations_ids_map = {simple_id: int(complex_id.split("_")[0])
                                    for complex_id, simple_id in self.relation_id_map.items()}
        result_ways_ids_map = {simple_id: int(complex_id.split("_")[0])
                               for complex_id, simple_id in self.way_id_map.items()}
        result_nodes_ids_map = {simple_id: int(complex_id.split("_")[0])
                                for complex_id, simple_id in self.node_id_map.items()}
        return result_nodes_ids_map, result_ways_ids_map, result_relations_ids_map

    def reset(self):
        self.relation_id_map.clear()
        self.way_id_map.clear()
        self.node_id_map.clear()
        self.id_counter = 1


class BatchManager(object):

    def __init__(self, gdal_batch_size, entities_number):
        self.entities_number = entities_number

        self.nodes_batch = {}
        self.ways_batch = {}
        self.all_relations_batch = {}
        self.main_relation_batch = {}
        self.ways_batch_counter = 0
        self.gdal_batch_size = gdal_batch_size

        self.id_manager = IdManager()

    def add_osm_dicts_to_batches(self, node_dicts_list=list(), way_dicts_list=list(), relation_dicts_list=list(),
                                 main_relation_dict=None):
        for node_dict in node_dicts_list:
            self.nodes_batch[node_dict["id"]] = node_dict
        for way_dict in way_dicts_list:
            self.ways_batch[way_dict["id"]] = way_dict
        for relation_dict in relation_dicts_list:
            self.all_relations_batch[relation_dict["id"]] = relation_dict
        if main_relation_dict is not None:
            self.all_relations_batch[main_relation_dict["id"]] = main_relation_dict
            self.main_relation_batch[main_relation_dict["id"]] = main_relation_dict
        self.ways_batch_counter = self.ways_batch_counter + 1

    def sorted_obj_batch_values(self, obj_batch):
        return sorted(list(obj_batch.values()), key=lambda obj: obj["id"])

    def get_batches_values_sorted_lists(self):
        return self.sorted_obj_batch_values(self.nodes_batch), \
               self.sorted_obj_batch_values(self.ways_batch), \
               self.sorted_obj_batch_values(self.all_relations_batch)

    def get_ways_simplified_ids(self):
        return list(self.ways_batch.keys())

    def get_main_relations_simplified_ids(self):
        return list(self.main_relation_batch.keys())

    def get_simplified_id_and_original_id_maps(self):
        return self.id_manager.get_simplified_id_and_original_id_maps()

    def replace_ids_in_way_and_its_dependencies(self, way_dict, way_nodes_dicts):
        self.id_manager.replace_ids_in_way_and_its_dependencies(way_dict, way_nodes_dicts)

    def replace_ids_in_relation_and_its_dependencies(self, relation_dict, relation_nodes_dicts,
                                                     relation_ways_dicts, relation_relations_dicts):
        self.id_manager.replace_ids_in_relation_and_its_dependencies(relation_dict, relation_nodes_dicts,
                                                                     relation_ways_dicts, relation_relations_dicts)

    def restore_ways_ids_and_add_geometry(self, id_geometry_map, result_func):
        result_nodes_ids_map, result_ways_ids_map, _ = self.id_manager.get_simplified_id_and_original_id_maps()

        for way_dict_id, way_dict in self.ways_batch.items():
            if way_dict["id"] in id_geometry_map:
                way_dict["geometry"] = json.dumps(id_geometry_map[way_dict["id"]])
            way_dict["id"] = result_ways_ids_map[way_dict["id"]]
            way_dict["nodes"] = [result_nodes_ids_map[node_id] if node_id in result_nodes_ids_map else node_id for
                                 node_id in way_dict["nodes"]]
            result_func(way_dict)

    def restore_relations_ids_and_add_geometry(self, id_geometry_map, result_func):
        result_nodes_ids_map, result_ways_ids_map, result_relations_ids_map = \
            self.id_manager.get_simplified_id_and_original_id_maps()

        for relation_dict_id, relation_dict in self.main_relation_batch.items():
            if relation_dict["id"] in id_geometry_map:
                relation_dict["geometry"] = json.dumps(id_geometry_map[relation_dict["id"]])
            relation_dict["id"] = result_relations_ids_map[relation_dict["id"]]
            self.id_manager.replace_ids_in_relation_members(relation_dict, result_nodes_ids_map,
                                                            result_ways_ids_map, result_relations_ids_map)
            result_func(relation_dict)

    def generate_batch_osm_file_name(self, work_dir, current_entity_type, current_index, pool_size):
        batch_end = current_index
        batch_start = batch_end - (self.get_batch_limit_for_current_entity(current_entity_type)*pool_size)
        return work_dir + '{}_{}_{}.osm'.format(current_entity_type, batch_start, batch_end)

    def is_full(self, entity_type):
        return self.ways_batch_counter >= self.get_batch_limit_for_current_entity(entity_type)

    def get_batch_limit_for_current_entity(self, entity_type):
        return self.gdal_batch_size if entity_type != "relations" else self.gdal_batch_size/2

    def reset(self):
        self.ways_batch_counter = 0
        self.nodes_batch.clear()
        self.ways_batch.clear()
        self.all_relations_batch.clear()
        self.main_relation_batch.clear()
        self.id_manager.reset()
