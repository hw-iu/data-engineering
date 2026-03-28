#!/usr/bin/env python3
from csv import DictReader
from json import dumps
from multiprocessing import Process
from os import environ
from pathlib import Path
from sys import (exit,
                 stderr,
                 stdout)

from time import sleep

from confluent_kafka import Producer

NEEDED_ENVIRONMENT_VARIABLES = [
    'DATA_DIRECTORY',
    'KAFKA_SERVERS',
    'KAFKA_TOPIC',
]

for needed_environment_variable in NEEDED_ENVIRONMENT_VARIABLES:
    if not environ.get(needed_environment_variable):
        exit(f'{needed_environment_variable} environment variable is not set')

data_directory_path = Path(environ.get('DATA_DIRECTORY'), '')
if not data_directory_path.exists():
    exit(f'{data_directory_path.name} does not exist')
if not data_directory_path.is_dir():
    exit(f'{data_directory_path.name} is not a directory')

kafka_servers = environ.get('KAFKA_SERVERS', 'localhost:9092')
kafka_topic = environ.get('KAFKA_TOPIC', 'incoming')

delay_between_messages = int(environ.get('DELAY_BETWEEN_MESSAGES', '0'))

debug = True if environ.get('DEBUG', 'false').lower() == 'true' else False

# allow logging from multiprocessing subprocesses
stdout.reconfigure(line_buffering=True)
stderr.reconfigure(line_buffering=True)


def producer_callback(error, message):
    if error:
        print(f'Error producing message: {error}')


def process_data_file(data_file: Path,
                      kafka_servers: str = '',
                      kafka_topic: str = '',
                      debug: bool = False):
    if debug:
        print(f'Found {data_file}')

    producer = Producer({
        'bootstrap.servers': f'{kafka_servers}'
    })

    with open(data_file) as data_file_object:
        reader = DictReader(data_file_object, delimiter=';')
        counter = 0
        kafka_key = data_file.stem.encode('utf-8')
        if debug:
            print(f'Using partition key: {kafka_key}')
        for row in reader:
            counter += 1
            # build payload with converted types where appropriate
            try:
                payload = {
                    'timestamp': str(row.get('timestamp', '')),
                    'site_id': int(float(row.get('site_id', 0))) if row.get('site_id', '') != '' else None,
                    'period_id': int(float(row.get('period_id', 0))) if row.get('period_id', '') != '' else None,
                    'actual_consumption': float(row.get('actual_consumption', 0.0)) if row.get('actual_consumption', '') != '' else None,
                    'actual_pv': float(row.get('actual_pv', 0.0)) if row.get('actual_pv', '') != '' else None,
                }
            except Exception as e:
                if debug:
                    print(f'Error converting row {counter}: {e} — row: {row}')
                continue

            # Send plain JSON (no Connect schema envelope) — kafka-consumer-enrich will add schema later
            value_bytes = dumps(payload).encode('utf-8')
            producer.produce(
                topic=kafka_topic,
                key=kafka_key,
                value=value_bytes,
                callback=producer_callback
            )
            producer.flush()
            sleep(delay_between_messages)
        print(f'finished {data_file_object.name} with {counter} rows')


if __name__ == '__main__':
    processes = list()
    for data_file in data_directory_path.glob('[0-9]*.csv'):
        process = Process(target=process_data_file, args=(data_file,
                                                          kafka_servers,
                                                          kafka_topic,
                                                          debug))
        if debug:
            print(f'Starting process for {data_file.name}')
        process.start()
        processes.append(process)

    # check if any process is still alive
    while [x for x in processes if x.is_alive()]:
        sleep(1)
