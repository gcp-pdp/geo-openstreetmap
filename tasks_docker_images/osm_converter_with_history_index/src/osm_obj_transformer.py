from datetime import datetime
import osmium


def osm_obj_to_dict(osm_entity, geometry, is_simplified, with_uid):
    base_dict = {
        "id": osm_entity.id,
        "version": osm_entity.version,
        "osm_timestamp": int(datetime.timestamp(osm_entity.timestamp)),
        "all_tags": [(tag.k, tag.v) for tag in osm_entity.tags]
    }
    if not is_simplified:
        base_dict["username"] = osm_entity.user
        base_dict["changeset"] = osm_entity.changeset
        base_dict["visible"] = osm_entity.visible
        base_dict["geometry"] = geometry
    if with_uid:
        base_dict["uid"] = osm_entity.uid
    return base_dict


def osm_entity_node_dict(osm_node_entity, geometry=None, is_simplified=False, with_uid=False):
    base_dict = osm_obj_to_dict(osm_node_entity, geometry, is_simplified, with_uid)
    try:
        base_dict["latitude"] = osm_node_entity.location.lat
        base_dict["longitude"] = osm_node_entity.location.lon
    except Exception as e:
        base_dict["latitude"] = None
        base_dict["longitude"] = None
    return base_dict


def osm_entity_way_dict(osm_way_entity, geometry=None, is_simplified=False, with_uid=False):
    base_dict = osm_obj_to_dict(osm_way_entity, geometry, is_simplified, with_uid)
    base_dict["nodes"] = [node.ref for node in osm_way_entity.nodes]
    return base_dict


def osm_entity_relation_dict(osm_relation_entity, geometry=None, is_simplified=False,  with_uid=False):
    base_dict = osm_obj_to_dict(osm_relation_entity, geometry, is_simplified, with_uid)
    base_dict["members"] = [(member.type, member.ref, member.role) for member in iter(osm_relation_entity.members)]
    return base_dict


def get_osm_obj_from_dict(obj_dict):
    return osmium.osm.mutable.OSMObject(id=obj_dict["id"],
                                        version=obj_dict["version"],
                                        visible=obj_dict["visible"] if "visible" in obj_dict else None,
                                        changeset=obj_dict["changeset"] if "changeset" in obj_dict else None,
                                        timestamp=datetime.fromtimestamp(obj_dict["osm_timestamp"]),
                                        uid=obj_dict["uid"] if "uid" in obj_dict else None,
                                        tags=obj_dict["all_tags"] if "all_tags" in obj_dict else None
                                        )


def get_osm_node_from_dict(node_dict):
    lon = node_dict["longitude"]
    lat = node_dict["latitude"]
    location_tuple = (lon, lat) if lon and lat else None
    return osmium.osm.mutable.Node(get_osm_obj_from_dict(node_dict), location_tuple)


def get_osm_way_from_dict(way_dict):
    return osmium.osm.mutable.Way(get_osm_obj_from_dict(way_dict), way_dict["nodes"])


def get_osm_relation_from_dict(relation_dict):
    return osmium.osm.mutable.Relation(get_osm_obj_from_dict(relation_dict), relation_dict["members"])


def edit_osm_obj_dict_according_to_bq_schema(obj_dict):
    obj_dict["all_tags"] = [{"key": tag_key, "value": tag_value} for tag_key, tag_value in obj_dict["all_tags"]]
    return obj_dict


def edit_node_dict_according_to_bq_schema(node_dict):
    return edit_osm_obj_dict_according_to_bq_schema(node_dict)


def edit_way_dict_according_to_bq_schema(way_dict):
    way_dict = edit_osm_obj_dict_according_to_bq_schema(way_dict)
    way_dict["nodes"] = [{"id": node_id} for node_id in way_dict["nodes"]]
    return way_dict


def edit_relation_dict_according_to_bq_schema(relation_dict):
    relation_dict = edit_osm_obj_dict_according_to_bq_schema(relation_dict)
    relation_dict["members"] = [{"type": member_type, "id": member_ref, "role": member_role}
                                for member_type, member_ref, member_role in relation_dict["members"]]
    return relation_dict


def is_node_dict_with_location(node_dict):
    return node_dict["longitude"] and node_dict["latitude"]


def get_way_nodes(way_dict):
    return way_dict["nodes"]


def get_relation_members(relation_dict):
    return relation_dict["members"]


