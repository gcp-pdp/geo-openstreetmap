FROM google/cloud-sdk

RUN cat /etc/os-release
# update repos
RUN apt-get update -y

# install pyosmium dependencies
RUN apt-get install build-essential cmake libboost-dev \
                   libexpat1-dev zlib1g-dev libbz2-dev -y
# install GDAL
RUN apt-get install binutils libproj-dev gdal-bin -y

# install python GCS sdk
RUN pip3 install --upgrade google-cloud-storage

# install pyosmium
RUN pip3 install osmium

# install guppy3 (memory profiler)
RUN pip3 install guppy3
# install psutil
RUN pip install psutil

# set env vars
ENV DATA_DIR /osm_converter_with_history_index/data/

# copy script files
COPY src /osm_converter_with_history_index/src
# set work dir
WORKDIR /osm_converter_with_history_index/src

# (Optional) GCP credetials setup fro local runs
#COPY keys /osm_converter_with_history_index/keys
#ENV GOOGLE_APPLICATION_CREDENTIALS=/osm_converter_with_history_index/keys/gcloud_keys.json

CMD python3 main.py $SRC_OSM_GCS_URI --index_db_and_metadata_gcs_dir $INDEX_DB_AND_METADATA_DIR_GCS_URI --converted_gcs_dir $CONVERTED_OSM_DIR_GCS_URI --num_db_shards $NUM_DB_SHARDS --num_threads $NUM_THREADS $ADDITIONAL_ARGS