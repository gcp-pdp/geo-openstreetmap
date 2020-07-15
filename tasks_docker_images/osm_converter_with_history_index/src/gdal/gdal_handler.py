import subprocess
import logging
import json
import time
import os

class GDALHandler(object):

    def __init__(self, script_path, config_path, work_dir):
        self.script_path = script_path
        self.config_path = config_path
        self.work_dir = work_dir
        self.type_layers = {"ways": ["lines", "multipolygons"],
                            "relations": ["multipolygons", "other_relations", "points", "multilinestrings", "lines"]}

    def osm_to_geojson(self, src_osm_filename, entity_type, result_ids):
        def geometry_from_geojson_features(geojson_features, feature_index):
            return geojson_features[feature_index]["properties"]["geometry"]
        try:
            file_size = os.path.getsize(src_osm_filename)
        except Exception:
            file_size = -1
        logging.info("Working with {}, size: {}".format(src_osm_filename, str(file_size)))
        start = time.time()

        id_geometry_map = {}
        layers = self.type_layers[entity_type]
        for layer in layers:
            temp_geojson_file_name = self.work_dir + "{}.geojson".format(layer)
            cmd = "sh {} {} {} {} {}".format(self.script_path, self.config_path, src_osm_filename,
                                             temp_geojson_file_name, layer)
            process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
            process.communicate()

            geojson_file = open(temp_geojson_file_name, "r")
            geojson_data = json.load(geojson_file)
            geojson_file.close()
            os.remove(temp_geojson_file_name)

            geojson_features = geojson_data["features"]
            if len(geojson_features) > 0:
                for index in range(len(geojson_features)):
                    current_id = geojson_features[index]["properties"]["osm_id"]
                    if not current_id:
                        current_id = geojson_features[index]["properties"]["osm_way_id"]
                    current_id = int(current_id)
                    if current_id in result_ids:
                        id_geometry_map[current_id] = geometry_from_geojson_features(geojson_features, index)
                        result_ids.remove(current_id)
                        if len(result_ids) == 0:
                            break
            if len(result_ids) == 0:
                break
        os.remove(src_osm_filename)
        logging.info("Finish working with {}. Time spent: {}s".format(src_osm_filename, (time.time() - start)))
        return id_geometry_map
