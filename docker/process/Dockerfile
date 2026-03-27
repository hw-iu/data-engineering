# Only Ubuntu 26.04 has newer librdkafka-dev package, so we use it as the base image for this container
FROM ubuntu:26.04
ARG DEBIAN_FRONTEND=noninteractive

RUN apt update
RUN apt install -y gcc \
                   librdkafka-dev \
                   python3 \
                   python3-pip

RUN pip install --break-system-packages --root-user-action=ignore quixstreams[postgresql]

RUN mkdir -p /app
WORKDIR /app
COPY main.py /app

CMD ["python3", "main.py"]
