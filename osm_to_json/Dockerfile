FROM osgeo/gdal

COPY src /osm_to_json/src
#COPY keys /osm_to_json/keys

# update repos
RUN apt-get update -y

# make installation
RUN apt-get install build-essential -y

# gsutil installation
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin
#RUN gcloud auth activate-service-account --key-file=/osm_to_json/keys/gcloud_keys.json

# perl installation
RUN curl -L http://xrl.us/installperlnix | bash
RUN cpan JSON
RUN cpan Text::CSV::Encoded

WORKDIR /osm_to_json/src
RUN ["chmod", "+x", "download_osm.sh"]
RUN ["chmod", "+x", "csv_to_json/csv-to-json.sh"]

#CMD ls -l /osm_to_json/
CMD gsutil cp gs://osm-dev-276509--osm-data/luxembourg.osm.pbf /osm_to_json/data/luxembourg.osm.pbf; \
    ls -l /osm_to_json/; \
    ls -l /osm_to_json/data; \
    ./osm2geojsoncsv /osm_to_json/data/luxembourg.osm.pbf /osm_to_json/data/converted; \
    ./csv_to_json/csv-to-json.sh /osm_to_json/data/; \
    gsutil cp /osm_to_json/data/*.jsonl gs://osm-dev-276509--osm-data/json/
