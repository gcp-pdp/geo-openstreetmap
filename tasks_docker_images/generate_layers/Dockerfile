FROM google/cloud-sdk

# copy script files
COPY src /generate_layers/src
#COPY keys /generate_layers/keys
# set work dir
WORKDIR /generate_layers/src

#RUN ["chmod", "+x", "layered_gis/00_generate_queries.sh"]
RUN ["chmod", "+x", "run.sh"]

#RUN gcloud auth activate-service-account --key-file=/generate_layers/keys/gcloud_keys.json

CMD ./run.sh
