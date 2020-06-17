from dataclasses import dataclass
from datetime import datetime

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