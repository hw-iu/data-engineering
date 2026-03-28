FROM python:3.14-slim
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update
RUN apt -y install curl \
                   unzip

RUN pip install --root-user-action=ignore playwright
RUN playwright install firefox
COPY curl-browser.py /usr/local/bin
RUN chmod +x /usr/local/bin/curl-browser.py
COPY create-cities-mapping.py /usr/local/bin
RUN chmod +x /usr/local/bin/create-cities-mapping.py

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# just in case no volume is mounted
RUN mkdir /data
VOLUME /data

ENTRYPOINT ["/docker-entrypoint.sh"]