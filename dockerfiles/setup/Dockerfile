FROM debian:13-slim
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update
RUN apt -y install curl \
                   unzip

# mapping.json is generated from https://simplemaps.com/static/data/country-cities/de/de.json
COPY mapping.json /usr/local/share/mapping.json

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# just in case no volume is mounted
RUN mkdir /data
VOLUME /data

ENTRYPOINT ["/docker-entrypoint.sh"]