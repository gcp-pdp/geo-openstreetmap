FROM osgeo/gdal

# update repos
RUN apt-get update -y

# make installation
RUN apt-get install build-essential -y

# gsutil installation
RUN curl -sSL https://sdk.cloud.google.com | bash

# perl installation
RUN curl -L http://xrl.us/installperlnix | bash
RUN cpan JSON
RUN cpan Text::CSV::Encoded

# set env vars
ENV PATH $PATH:/root/google-cloud-sdk/bin
ENV DATA_DIR /osm_to_features/data/

# copy script files
COPY src /osm_to_features/src
# set work dir
WORKDIR /osm_to_features/src

# set sh files as executable
RUN ["chmod", "+x", "download_osm.sh"]
RUN ["chmod", "+x", "csv_to_json/csv-to-json.sh"]
RUN ["chmod", "+x", "osm_to_features.sh"]

# run main script
CMD ./osm_to_features.sh
