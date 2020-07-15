import sqlite3
import json
import logging
import time

logging.getLogger().setLevel(logging.INFO)


class OsmIndex(object):
    def __init__(self):
        pass

    def create(self):
        pass

    def save(self):
        pass

    def close(self):
        pass


class SQLiteOsmIndex(OsmIndex):

    def __init__(self, db_file_path):
        super().__init__()
        self.db_file_path = db_file_path
        self.sqlite3_connection = sqlite3.connect(db_file_path)
        self.osm_index_db_cursor = self.sqlite3_connection.cursor()
        self.query_time = 0
        self.query_counter = 0

    def get_db_file_path(self):
        return self.db_file_path

    def get_query_time(self):
        return self.query_time

    def get_query_counter(self):
        return self.query_counter

    def create(self):
        self.init_nodes_table()
        self.init_ways_table()
        self.init_relations_table()

    def save(self):
        self.sqlite3_connection.commit()

    def close(self):
        self.save()
        self.sqlite3_connection.close()

    def init_nodes_table(self):
        self.osm_index_db_cursor.execute('''CREATE TABLE nodes
                         (id INT, version INT, osm_timestamp INT, all_tags TEXT, lon REAL, lat REAL)''')
        self.osm_index_db_cursor.execute('''CREATE INDEX idx_nodes_id_version
                         ON nodes (id, version)''')

        self.save()

    def init_ways_table(self):
        self.osm_index_db_cursor.execute('''CREATE TABLE ways
                         (id INT, version INT, osm_timestamp INT, all_tags TEXT, nodes TEXT)''')
        self.osm_index_db_cursor.execute('''CREATE INDEX idx_ways_id_version
                         ON ways (id, version)''')
        self.save()

    def init_relations_table(self):
        self.osm_index_db_cursor.execute('''CREATE TABLE relations
                         (id INT, version INT, osm_timestamp INT, all_tags TEXT, members TEXT)''')
        self.osm_index_db_cursor.execute('''CREATE INDEX idx_relations_id_version
                         ON relations (id, version)''')
        self.save()

    def execute_query(self, query, values=None):
        if values:
            self.osm_index_db_cursor.execute(query, values)
        else:
            self.osm_index_db_cursor.execute(query)

    def add_values_to_sqlite_table(self, table_name, values):
        placeholders = ",".join(["?"] * len(values))
        query = "INSERT INTO {} VALUES ({})".format(table_name, placeholders)
        self.execute_query(query, values)

    def get_id_version_timestamp_all_tags_from_osm_obj(self, osm_obj):
        return str(osm_obj["id"]), str(osm_obj["version"]), \
               str(osm_obj["osm_timestamp"]), \
               json.dumps(osm_obj["all_tags"]) if "all_tags" in osm_obj and len(osm_obj["all_tags"]) > 0 else "[]"

    def add_node_to_index(self, node_dict):
        osm_id, ver, timestamp, all_tags = self.get_id_version_timestamp_all_tags_from_osm_obj(node_dict)
        lon = node_dict["longitude"]
        lat = node_dict["latitude"]
        self.add_values_to_sqlite_table("nodes", [osm_id, ver, timestamp, all_tags, lon, lat])

    def add_way_to_index(self, way_dict):
        osm_id, ver, timestamp, all_tags = self.get_id_version_timestamp_all_tags_from_osm_obj(way_dict)
        node_ids = json.dumps(way_dict["nodes"])
        # self.add_values_to_sqlite_table("ways", [osm_id, ver, timestamp, all_tags, node_ids])

    def add_relation_to_index(self, relation_dict):
        osm_id, ver, timestamp, all_tags = self.get_id_version_timestamp_all_tags_from_osm_obj(relation_dict)
        members = json.dumps(relation_dict["members"])
        # self.add_values_to_sqlite_table("relations", [osm_id, ver, timestamp, all_tags, members])

    def get_row_from_index_by_timestamp(self, table_name, id, timestamp):
        query = "SELECT * FROM {} table_name WHERE id={} AND osm_timestamp<{} ORDER BY osm_timestamp DESC" \
            .format(table_name, id, timestamp)
        self.execute_query(query)
        return self.osm_index_db_cursor.fetchone()

    def get_node_from_index_by_timestamp(self, node_id, timestamp):
        node_data = self.get_row_from_index_by_timestamp("nodes", node_id, timestamp)
        if not node_data:
            return

        node_fields = ["id", "version", "osm_timestamp", "all_tags", "longitude", "latitude"]
        node_dict = {}
        for index in range(len(node_fields)):
            if node_fields[index] == "all_tags":
                node_dict[node_fields[index]] = json.loads(node_data[index])
            else:
                node_dict[node_fields[index]] = node_data[index]

        return node_dict

    def get_way_from_index_by_timestamp(self, way_id, timestamp):
        way_data = self.get_row_from_index_by_timestamp("ways", way_id, timestamp)
        if not way_data:
            return

        way_fields = ["id", "version", "osm_timestamp", "all_tags"]
        way_dict = {}
        for index in range(len(way_fields)):
            if way_fields[index] == "all_tags":
                way_dict[way_fields[index]] = json.loads(way_data[index])
            else:
                way_dict[way_fields[index]] = way_data[index]

        way_dict["nodes"] = json.loads(way_data[len(way_data) - 1])
        return way_dict

    def get_relation_from_index_by_timestamp(self, relation_id, timestamp):
        relation_data = self.get_row_from_index_by_timestamp("relations", relation_id, timestamp)
        if not relation_data:
            return

        relation_fields = ["id", "version", "osm_timestamp", "all_tags"]
        relation_dict = {}
        for index in range(len(relation_fields)):
            if relation_fields[index] == "all_tags":
                relation_dict[relation_fields[index]] = json.loads(relation_data[index])
            else:
                relation_dict[relation_fields[index]] = relation_data[index]

        relation_dict["members"] = json.loads(relation_data[len(relation_data) - 1])
        return relation_dict

    def merge_identical_db(self, db_file_to_merge):
        tables = ["nodes", "ways", "relations"]
        db_to_merge_temp_name = "dbToMerge"
        self.execute_query("ATTACH '{}' as {}".format(db_file_to_merge, db_to_merge_temp_name))
        for table in tables:
            self.execute_query("INSERT into {} SELECT * FROM {}.{}"
                                            .format(table, db_to_merge_temp_name, table))
            self.sqlite3_connection.commit()
        self.execute_query("DETACH {}".format(db_to_merge_temp_name))

def merge_dbs(new_db_file, db_paths):
    new_db = SQLiteOsmIndex(new_db_file)
    new_db.create()
    for db_path in db_paths:
        logging.info("Merging {} into {}".format(db_path, new_db_file))
        new_db.merge_identical_db(db_path)
    new_db.close()
    return new_db_file
