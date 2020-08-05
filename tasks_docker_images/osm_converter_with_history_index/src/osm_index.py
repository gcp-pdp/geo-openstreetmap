import sqlite3
import json
import logging
import time


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

        self.tables_and_fields = {"nodes": {
            "id": "INT",
            "version": "INT",
            "osm_timestamp": "INT",
            "longitude": "REAL",
            "latitude": "REAL",
        }, "ways": {
            "id": "INT",
            "version": "INT",
            "osm_timestamp": "INT",
            "nodes": "TEXT"
        }, "relations": {
            "id": "INT",
            "version": "INT",
            "osm_timestamp": "INT",
            "nodes": "TEXT"
        }}

        self.nodes_fields_list = list(self.tables_and_fields["nodes"].keys())
        self.ways_fields_list = list(self.tables_and_fields["ways"].keys())
        self.relations_fields_list = list(self.tables_and_fields["relations"].keys())

        self.nodes_fields_str = ",".join(self.nodes_fields_list)
        self.ways_fields_str = ",".join(self.ways_fields_list)
        self.relations_fields_str = ",".join(self.relations_fields_list)

    def get_db_file_path(self):
        return self.db_file_path

    def get_query_time(self):
        return self.query_time

    def reset_query_time(self):
        self.query_time = 0

    def get_query_counter(self):
        return self.query_counter

    def reset_query_counter(self):
        self.query_counter = 0

    def create(self):
        self.init_all_tables()

    def save(self):
        self.sqlite3_connection.commit()

    def close(self):
        self.save()
        self.sqlite3_connection.close()

    def init_all_tables(self):
        for table, fields_dicts in self.tables_and_fields.items():
            fields = ["{} {}".format(field_name, field_type) for field_name, field_type in fields_dicts.items()]
            self.osm_index_db_cursor.execute('CREATE TABLE {} ({})'.format(table, ",".join(fields)))
            self.osm_index_db_cursor.execute('CREATE INDEX idx_{}_id_version ON {} (id, version)'.format(table, table))
            self.save()

    def execute_query(self, query, values=None):
        query_start_timestamp = time.time()
        if values:
            self.osm_index_db_cursor.execute(query, values)
        else:
            self.osm_index_db_cursor.execute(query)
        self.query_time += (time.time() - query_start_timestamp)
        self.query_counter += 1

    def add_values_to_sqlite_table(self, table_name, values):
        placeholders = ",".join(["?"] * len(values))
        query = "INSERT INTO {} VALUES ({})".format(table_name, placeholders)
        self.execute_query(query, values)

    def get_id_version_timestamp_all_tags_from_osm_obj(self, osm_obj):
        return str(osm_obj["id"]), str(osm_obj["version"]), str(osm_obj["osm_timestamp"])

    def add_node_to_index(self, node_dict):
        osm_id, ver, timestamp = self.get_id_version_timestamp_all_tags_from_osm_obj(node_dict)
        lon = node_dict["longitude"] if "longitude" in node_dict else None
        lat = node_dict["latitude"] if "latitude" in node_dict else None
        self.add_values_to_sqlite_table("nodes", [osm_id, ver, timestamp, lon, lat])

    def add_way_to_index(self, way_dict):
        osm_id, ver, timestamp = self.get_id_version_timestamp_all_tags_from_osm_obj(way_dict)
        node_ids = json.dumps(way_dict["nodes"])
        self.add_values_to_sqlite_table("ways", [osm_id, ver, timestamp, node_ids])

    def add_relation_to_index(self, relation_dict):
        osm_id, ver, timestamp = self.get_id_version_timestamp_all_tags_from_osm_obj(relation_dict)
        members = json.dumps(relation_dict["members"])
        self.add_values_to_sqlite_table("relations", [osm_id, ver, timestamp, members])

    def get_row_from_index_by_timestamp(self, table_name, id, timestamp, fields_str=None):
        query = "SELECT {} FROM {} table_name WHERE id={} AND osm_timestamp<{} ORDER BY osm_timestamp DESC" \
            .format(fields_str if fields_str else "*", table_name, id, timestamp)
        self.execute_query(query)
        return self.osm_index_db_cursor.fetchone()

    def get_node_from_index_by_timestamp(self, node_id, timestamp):
        node_data = self.get_row_from_index_by_timestamp("nodes", node_id, timestamp)
        if not node_data:
            return

        node_dict = {field: node_data[index] for index, field in enumerate(self.nodes_fields_list)}
        return node_dict

    def get_way_from_index_by_timestamp(self, way_id, timestamp):
        way_data = self.get_row_from_index_by_timestamp("ways", way_id, timestamp)
        if not way_data:
            return

        way_dict = {}
        for index, field in enumerate(self.ways_fields_list):
            if field == "nodes":
                way_dict["nodes"] = json.loads(way_data[index])
            else:
                way_dict[field] = way_data[index]
        return way_dict

    def get_relation_from_index_by_timestamp(self, relation_id, timestamp):
        relation_data = self.get_row_from_index_by_timestamp("relations", relation_id, timestamp)
        if not relation_data:
            return

        relation_dict = {}
        for index, field in enumerate(self.relations_fields_list):
            if field == "members":
                relation_dict["members"] = json.loads(relation_data[index])
            else:
                relation_dict[field] = relation_data[index]
        return relation_dict

    def merge_identical_db(self, db_file_to_merge):
        tables = list(self.tables_and_fields.keys())
        db_to_merge_temp_name = "dbToMerge"
        self.execute_query("ATTACH '{}' as {}".format(db_file_to_merge, db_to_merge_temp_name))
        for table in tables:
            self.execute_query("INSERT into {} SELECT * FROM {}.{}"
                               .format(table, db_to_merge_temp_name, table))
            self.sqlite3_connection.commit()
        self.execute_query("DETACH {}".format(db_to_merge_temp_name))


def merge_dbs(new_db_file, db_paths, db_exists):
    new_db = SQLiteOsmIndex(new_db_file)
    if not db_exists:
        new_db.create()
    for db_path in db_paths:
        logging.info("Merging {} into {}".format(db_path, new_db_file))
        new_db.merge_identical_db(db_path)
    new_db.close()
    return new_db_file
