FROM google/cloud-sdk

# copy script files
COPY src /generate_layers/src
# set work dir
WORKDIR /generate_layers/src

RUN ["chmod", "+x", "run.sh"]

CMD ./run.sh

