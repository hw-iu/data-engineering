#!/usr/bin/env python3
# ©2026 Henri Wahl
#
# Get topic from Kafka and enrich photovoltaic data with geographic information
#

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

# variables which have to be defined, for example in docker-compose.yml
NEEDED_ENVIRONMENT_VARIABLES = [
    'CONSUMER_GROUP_ID',
    'DATA_DIRECTORY',
    'ENRICH_CONSUMER_COUNT',
    'ENRICH_MESSAGES_COUNT',
    'KAFKA_SERVERS',
    'KAFKA_TOPIC_INPUT',
    'KAFKA_TOPIC_OUTPUT',
]

# break if not all necessary variables are given
for needed_environment_variable in NEEDED_ENVIRONMENT_VARIABLES:
    if not environ.get(needed_environment_variable):
        exit(f'{needed_environment_variable} environment variable is not set')

# keys from the cities static data which are not needed for the enrichment process and can be removed
UNNEEDED_CITY_STATIC_DATA_PROPERTIES = ['admin_name', 'capital', 'iso2', 'population', 'population_proper']

# DATA_DIRECTORY is the path where the data files for enrichment are expected to be found
data_directory_path = Path(environ.get('DATA_DIRECTORY'), '')
if not data_directory_path.exists():
    exit(f'{data_directory_path.name} does not exist')
if not data_directory_path.is_dir():
    exit(f'{data_directory_path.name} is not a directory')

# retrieve all further configuration from environment variables
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

# get cities data
cities_ids_mapping_path = data_directory_path / 'cities_ids' / 'mapping.json'
if not cities_ids_mapping_path.exists():
    exit(f'{cities_ids_mapping_path.name} does not exist')
with open(cities_ids_mapping_path) as cities_ids_mapping_file:
    cities_ids_mapping = load(cities_ids_mapping_file)

# get geographical data
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

# create fake mappings of photovoltaic site IDs to city names
# this dictionary will be used for enriching photovoltaic data which only
# contains a site ID
cities_static_data_by_id = dict()
cities_static_data_temporary = deepcopy(cities_static_data_raw)
for city_id, city_name in cities_ids_mapping.items():
    for city_static_data in cities_static_data_temporary:
        if city_static_data.get('city') and \
                city_name == city_static_data['city']:
            # throw away completely unneeded information
            for unneeded_city_static_data_property in UNNEEDED_CITY_STATIC_DATA_PROPERTIES:
                if unneeded_city_static_data_property in city_static_data.keys():
                    del city_static_data[unneeded_city_static_data_property]
            cities_static_data_by_id[int(city_id)] = city_static_data
            cities_static_data_temporary.remove(city_static_data)


def convert_timestamp_to_epoch_ms(timestamp_string):
    """
    Convert a timestamp string in the format 'YYYY-MM-DDTHH:MM:SS' to epoch milliseconds
    This is needed for quixtreams data aggregations
    :param timestamp_string:
    """
    try:
        date_time = datetime.fromisoformat(timestamp_string)
        epoch_milliseconds = int(date_time.timestamp() * 1000)
        return epoch_milliseconds
    except Exception as error:
        print(f'Error converting timestamp: {error} — timestamp_str: {timestamp_string}')
        return None


def producer_callback(error, message):
    """
    Callback function for producer - logs error if any occurs and debugs otherwise if needed
    :param error:
    :param message:
    :return:
    """
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
    """
    Consumes messages from Kafka broker, enriches them and produces them back into
    the Kafka topic for enriched messages

    The enrichment consists of added city data

    :param group_id:
    :param kafka_servers:
    :param kafka_topic_input:
    :param kafka_topic_output:
    :param consumer_number:
    :param cities_static_data_by_id:
    :param debug:
    :return:
    """

    print(f"Starting consumer process {consumer_number} in group '{group_id}'")

    # reads raw/enriched messages
    consumer = Consumer({
        'bootstrap.servers': f'{kafka_servers}',
        'group.id': f'{consumer_group_id}',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([f'{kafka_topic_input}'])

    # writes enriched output
    producer = Producer({
        'bootstrap.servers': f'{kafka_servers}'
    })

    print('Listening for messages...')
    try:
        while True:
            # pull a small batch to reduce overhead while keeping latency low
            messages = consumer.consume(num_messages=enrich_messages_count,
                                        timeout=1.0)
            for message in messages:
                # parse incoming value
                raw = message.value()
                try:
                    value = loads(raw.decode('utf-8'))
                except Exception:
                    # if parsing fails, skip
                    if debug:
                        print('Failed to parse message, skipping')
                    continue

                # enrich the payload with static geo information based on site_id
                value.update(cities_static_data_by_id.get(value.get('site_id')))

                # build the final payload and normalize types for storage/analytics
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
            # commit offsets only after successful production of the batch
            consumer.commit()
    finally:
        consumer.close()


if __name__ == '__main__':
    # One list to hold all processes
    processes = list()

    # depending on how many enrichement processes are defined in
    # ENRICH_CONSUMER_COUNT, start that many processes
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
