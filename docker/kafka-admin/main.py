#!/usr/bin/env python3
# ©2026 Henri Wahl
#
# Setting up the Kafka cluster with the replication, partitions and topics needed later on
#


from os import environ
from confluent_kafka.admin import AdminClient, NewTopic

# variables which have to be defined, for example in docker-compose.yml
NEEDED_ENVIRONMENT_VARIABLES = [
    'KAFKA_SERVERS',
    'KAFKA_TOPICS',
]

# break if not all necessary variables are given
for needed_environment_variable in NEEDED_ENVIRONMENT_VARIABLES:
    if not environ.get(needed_environment_variable):
        exit(f'{needed_environment_variable} environment variable is not set')

kafka_servers = environ.get('KAFKA_SERVERS', 'localhost:9092')
# topics will come in as a string where each topic is on a new line, prefixed with a dash (e.g. from a YAML list)
# these dashes need to be cleaned out here
kafka_topics = [x.lstrip('-').lstrip() for x in environ.get('KAFKA_TOPICS', 'incoming').splitlines()]
kafka_partitions = int(environ.get('KAFKA_PARTITIONS', 1))
kafka_replication_factor = int(environ.get('KAFKA_REPLICATION_FACTOR', 3))

admin_conf = {'bootstrap.servers': f'{kafka_servers}', 'client.id': 'kafka-admin'}
kafka_admin = AdminClient(admin_conf)

print(f'Configured topics: {kafka_topics}')
print(f'Configured partitions: {kafka_partitions}')
print(f'Configured replication_factor: {kafka_replication_factor}')

# check for existing topics
metadata = kafka_admin.list_topics(timeout=10)
topics_existing = set(metadata.topics.keys()) if metadata and metadata.topics else set()
if topics_existing:
    print(f'Already existing topics: {topics_existing}')

# create non-existing topics
for kafka_topic in kafka_topics:
    if kafka_topic not in topics_existing:
        print(f'Creating topic: {kafka_topic}')
        new_topic = NewTopic(kafka_topic, num_partitions=kafka_partitions, replication_factor=kafka_replication_factor)
        futures = kafka_admin.create_topics([new_topic], request_timeout=15)
        future = futures.get(kafka_topic)
        try:
            result = future.result(timeout=30)
            print(f'Topic {kafka_topic} created: {result}')
        except Exception as exc:
            print(f'Failed to create topic {kafka_topic}: {exc}')
    else:
        print(f'Topic {kafka_topic} already exists')
