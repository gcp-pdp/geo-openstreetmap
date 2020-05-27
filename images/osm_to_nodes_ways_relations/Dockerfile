FROM google/cloud-sdk

# update repos
RUN apt-get update -y

# install pyosmium dependencies
RUN apt-get install build-essential cmake libboost-dev \
                   libexpat1-dev zlib1g-dev libbz2-dev -y
# install python GCS sdk
RUN pip3 install --upgrade google-cloud-storage

# install pyosmium
RUN pip3 install osmium

# set env vars
ENV DATA_DIR /osm_to_nodes_ways_relations/data/

# copy script files
COPY src /osm_to_nodes_ways_relations/src
# set work dir
WORKDIR /osm_to_nodes_ways_relations/src

CMD python3 pbf_parser.py $SRC_OSM_GCS_URI $NODES_WAYS_RELATIONS_DIR_GCS_URI
