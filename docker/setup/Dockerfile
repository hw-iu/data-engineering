FROM python:3.14-slim
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update
RUN apt -y install curl \
                   unzip

# playwright is needed for getting the simplemaps JSON file, because it is only downloadable by a Javascript
# capabable browser - which renders curl useless here
RUN pip install --break-system-packages --root-user-action=ignore playwright
RUN playwright install firefox
# curl functionality is achieved here with playwright
COPY curl-browser.py /usr/local/bin
RUN chmod +x /usr/local/bin/curl-browser.py
# create some random mapping between ascending IDs and city names from simplmaps
COPY create-cities-mapping.py /usr/local/bin
RUN chmod +x /usr/local/bin/create-cities-mapping.py

# docker-entrypoint.sh will download data files from Kaggle and srun the above two scripts
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# just in case no volume is mounted
RUN mkdir /data
# the volume which will contain the later processed data
VOLUME /data

ENTRYPOINT ["/docker-entrypoint.sh"]