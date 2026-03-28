#!/usr/bin/env python3
# ©2026 Henri Wahl

from copy import deepcopy
from datetime import datetime
from json import (dumps,
                  load,
                  loads)
from multiprocessing import Process
from os import environ
from pathlib import Path
from sys import (exit,
                 stderr,
                 stdout)

from confluent_kafka import (Consumer,
                             Producer)


NEEDED_ENVIRONMENT_VARIABLES = [
    'CONSUMER_GROUP_ID',
    'DATA_DIRECTORY',
    'ENRICH_CONSUMER_COUNT',
    'ENRICH_MESSAGES_COUNT',
    'KAFKA_SERVERS',
    'KAFKA_TOPIC_INPUT',
    'KAFKA_TOPIC_OUTPUT',
]

UNNEEDED_CITY_STATIC_DATA_PROPERTIES = ['admin_name', 'capital', 'iso2', 'population', 'population_proper']

for needed_environment_variable in NEEDED_ENVIRONMENT_VARIABLES:
    if not environ.get(needed_environment_variable):
        exit(f'{needed_environment_variable} environment variable is not set')

data_directory_path = Path(environ.get('DATA_DIRECTORY'), '')
if not data_directory_path.exists():
    exit(f'{data_directory_path.name} does not exist')
if not data_directory_path.is_dir():
    exit(f'{data_directory_path.name} is not a directory')

consumer_group_id = environ.get('CONSUMER_GROUP_ID', 'incoming_group')
enrich_consumer_count = int(environ.get('ENRICH_CONSUMER_COUNT', '1'))
enrich_messages_count = int(environ.get('ENRICH_MESSAGES_COUNT', '5'))

kafka_servers = environ.get('KAFKA_SERVERS', 'localhost:9092')
kafka_topic_input = environ.get('KAFKA_TOPIC_INPUT', 'incoming')
kafka_topic_output = environ.get('KAFKA_TOPIC_OUTPUT', 'outgoing')

debug = True if environ.get('DEBUG', 'false').lower() == 'true' else False

# allow logging from multiprocessing subprocesses
stdout.reconfigure(line_buffering=True)
stderr.reconfigure(line_buffering=True)

cities_ids_mapping_path = data_directory_path / 'cities_ids' / 'mapping.json'
if not cities_ids_mapping_path.exists():
    exit(f'{cities_ids_mapping_path.name} does not exist')
with open(cities_ids_mapping_path) as cities_ids_mapping_file:
    cities_ids_mapping = load(cities_ids_mapping_file)

cities_static_data_path = data_directory_path / 'simplemaps' / 'de.json'
if not cities_static_data_path.exists():
    exit(f'{cities_static_data_path.name} does not exist')
with open(cities_static_data_path) as cities_static_data_file:
    cities_static_data_raw = load(cities_static_data_file)

# cleanup cities static data to only keep the properties of interest for the enrichment process
city_properties_of_interest = ['city', 'lat', 'lng', 'country']
cities_static_data = list()
for city in cities_static_data_raw:
    cities_static_data.append({k: city[k] for k in city_properties_of_interest if k in city.keys()})

# ...mapping logic unchanged...
cities_static_data_by_id = dict()
cities_static_data_temporary = deepcopy(cities_static_data_raw)
for city_id, city_name in cities_ids_mapping.items():
    for city_static_data in cities_static_data_temporary:
        if city_static_data.get('city') and \
                city_name == city_static_data['city']:
            for unneeded_city_static_data_property in UNNEEDED_CITY_STATIC_DATA_PROPERTIES:
                if unneeded_city_static_data_property in city_static_data.keys():
                    del city_static_data[unneeded_city_static_data_property]
            cities_static_data_by_id[int(city_id)] = city_static_data
            cities_static_data_temporary.remove(city_static_data)


def convert_timestamp_to_epoch_ms(timestamp_string):
    """
    Convert a timestamp string in the format 'YYYY-MM-DDTHH:MM:SS' to epoch milliseconds.
    """
    try:
        date_time = datetime.fromisoformat(timestamp_string)
        epoch_milliseconds = int(date_time.timestamp() * 1000)
        return epoch_milliseconds
    except Exception as error:
        print(f'Error converting timestamp: {error} — timestamp_str: {timestamp_string}')
        return None


def producer_callback(error, message):
    if error:
        print(f'Error producing message: {error}')
    else:
        if debug: print(message.value())


def process_consume_messages(group_id: str = 'default',
                             kafka_servers: str = 'localhost:9092',
                             kafka_topic_input: str = 'incoming',
                             kafka_topic_output: str = 'outgoing',
                             consumer_number: int = 1,
                             cities_static_data_by_id: dict = dict(),
                             debug: bool = False):
    print(f"Starting consumer process {consumer_number} in group '{group_id}'")

    consumer = Consumer({
        'bootstrap.servers': f'{kafka_servers}',
        'group.id': f'{consumer_group_id}',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([f'{kafka_topic_input}'])

    producer = Producer({
        'bootstrap.servers': f'{kafka_servers}'
    })

    print('Listening for messages...')
    try:
        while True:
            messages = consumer.consume(num_messages=enrich_messages_count,
                                        timeout=1.0)
            for message in messages:
                # parse incoming value (assume it's JSON payload or Connect-envelope)
                raw = message.value()
                try:
                    value = loads(raw.decode('utf-8'))
                except Exception:
                    # if parsing fails, skip
                    if debug:
                        print('Failed to parse message, skipping')
                    continue

                # enrich
                value.update(cities_static_data_by_id.get(value.get('site_id')))

                # build payload with type conversions
                try:
                    payload = {
                        'timestamp': str(value.get('timestamp', '')),
                        'timestamp_epoch_ms': convert_timestamp_to_epoch_ms(value.get('timestamp', '')),
                        'site_id': int(float(value.get('site_id'))) if value.get('site_id') not in (None, '') else None,
                        'period_id': int(float(value.get('period_id'))) if value.get('period_id') not in (None, '') else None,
                        'actual_consumption': float(value.get('actual_consumption')) if value.get('actual_consumption') not in (None, '') else None,
                        'actual_pv': float(value.get('actual_pv')) if value.get('actual_pv') not in (None, '') else None,
                        'city': value.get('city'),
                        'lat': str(value.get('lat')) if value.get('lat') is not None else None,
                        'lng': str(value.get('lng')) if value.get('lng') is not None else None,
                        'country': value.get('country')
                    }
                    if debug: print(f'payload: {payload}')
                except Exception as error:
                    if debug: print(f'error converting/enriching payload: {error} — value: {value}')
                    continue

                value_bytes = dumps(payload).encode('utf-8')
                producer.produce(
                    topic=kafka_topic_output,
                    key=message.key(),
                    value=value_bytes,
                    callback=producer_callback
                )
            # flush in batches
            producer.flush()
            # tell Kafka about the successful consumption
            consumer.commit()
    finally:
        consumer.close()


if __name__ == '__main__':
    # One list to hold all processes
    processes = list()

    for consumer_number in range(1, enrich_consumer_count + 1):
        process = Process(target=process_consume_messages,
                          args=(consumer_group_id,
                                kafka_servers,
                                kafka_topic_input,
                                kafka_topic_output,
                                consumer_number,
                                cities_static_data_by_id,
                                debug))
        processes.append(process)
        process.start()
