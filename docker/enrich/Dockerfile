FROM python:3.14-slim
ARG DEBIAN_FRONTEND=noninteractive

RUN pip install --root-user-action=ignore confluent_kafka

RUN mkdir -p /app
WORKDIR /app
COPY main.py /app

ENTRYPOINT ["python3", "main.py"]
