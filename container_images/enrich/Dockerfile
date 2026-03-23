FROM python:3.14-slim

RUN pip install confluent_kafka

RUN mkdir -p /app
WORKDIR /app
COPY main.py /app

ENTRYPOINT ["python3", "main.py"]
