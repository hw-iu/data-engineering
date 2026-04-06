#!/usr/bin/env python3
# ©2026 Henri Wahl
#
# Doing some aggregations with data from consumed Kafka topic and store it into databases
#

from datetime import timedelta
from multiprocessing import Process
from os import environ
from sys import (exit,
                 stderr,
                 stdout)
from typing import Any, Optional, List, Tuple

from quixstreams import Application
from quixstreams.dataframe.windows import (Count,
                                           Max,
                                           Mean)
from quixstreams.models import TimestampType
from quixstreams.sinks.community.postgresql import PostgreSQLSink

# variables which have to be defined, for example in docker-compose.yml
NEEDED_ENVIRONMENT_VARIABLES = [
    'CONSUMER_GROUP_ID',
    'PROCESS_CONSUMER_COUNT',
    'KAFKA_SERVERS',
    'KAFKA_TOPIC_INPUT',
    'DB_USER',
    'DB_PASSWORD',
    'DB_DATABASE'
]

# break if not all necessary variables are given
for needed_environment_variable in NEEDED_ENVIRONMENT_VARIABLES:
    if not environ.get(needed_environment_variable):
        exit(f'{needed_environment_variable} environment variable is not set')

# retrieve all further configuration from environment variables
consumer_group_id = environ.get('CONSUMER_GROUP_ID', 'incoming_group')
process_consumer_count = int(environ.get('PROCESS_CONSUMER_COUNT', '1'))
kafka_servers = environ.get('KAFKA_SERVERS', 'localhost:9092')
kafka_topic_input = environ.get('KAFKA_TOPIC_INPUT', 'incoming')

db_user = environ.get('DB_USER', 'user')
db_password = environ.get('DB_PASSWORD', 'password')
db_database = environ.get('DB_DATABASE', 'database')

debug = True if environ.get('DEBUG', 'false').lower() == 'true' else False

# allow logging from multiprocessing subprocesses
stdout.reconfigure(line_buffering=True)
stderr.reconfigure(line_buffering=True)


def get_timestamp_epoch_ms(
        value: Any,
        headers: Optional[List[Tuple[str, bytes]]],
        timestamp: float,
        timestamp_type: TimestampType) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload
    instead of Kafka timestamp - must use this signature
    """
    return value['timestamp_epoch_ms']


def process_process_messages(group_id: str = 'default',
                             kafka_servers: str = 'localhost:9092',
                             kafka_topic_input: str = 'incoming',
                             consumer_number: int = 1,
                             debug: bool = False):
    """


    :param group_id:
    :param kafka_servers:
    :param kafka_topic_input:
    :param consumer_number:
    :param debug:
    :return:
    """

    print(f"Starting consumer process {consumer_number} in group '{group_id}'")

    # Quixstreams application, which will consume an aggregate data
    app = Application(
        broker_address=kafka_servers,
        consumer_group=kafka_topic_input,
        auto_offset_reset='earliest',
        use_changelog_topics=False,
        state_dir=f'/tmp/{group_id}_{consumer_number}'
    )

    if debug: print(app)

    topic_input = app.topic(name=kafka_topic_input, timestamp_extractor=get_timestamp_epoch_ms)
    if debug: print(topic_input)

    # Initialize PostgreSQLSinks - one for every aggregation
    # sink for hourly aggregations
    postgres_sink_hourly_aggregations = PostgreSQLSink(
        host='database',
        port=5432,
        dbname=db_database,
        user=db_user,
        password=db_password,
        table_name='hourly_aggregations',
        schema_auto_update=True,
        primary_key_columns=['city', 'timestamp'],
        upsert_on_primary_key=True
    )
    if debug: print(postgres_sink_hourly_aggregations)

    # sink for weekly aggregations
    postgres_sink_daily_aggregations = PostgreSQLSink(
        host='database',
        port=5432,
        dbname=db_database,
        user=db_user,
        password=db_password,
        table_name='daily_aggregations',
        schema_auto_update=True,
        primary_key_columns=['city', 'timestamp'],
        upsert_on_primary_key=True
    )
    if debug: print(postgres_sink_daily_aggregations)

    # sink for weekly aggregations
    postgres_sink_weekly_aggregations = PostgreSQLSink(
        host='database',
        port=5432,
        dbname=db_database,
        user=db_user,
        password=db_password,
        table_name='weekly_aggregations',
        schema_auto_update=True,
        primary_key_columns=['city', 'timestamp'],
        upsert_on_primary_key=True
    )
    if debug: print(postgres_sink_weekly_aggregations)

    # nice concept of dataframe is applicable here, spanning it along the topic input from Kafka
    dataframe = app.dataframe(topic=topic_input).group_by('city')
    if debug: print(dataframe)

    # hourly aggregation of max and mean values of actual power production
    dataframe_hourly_aggregations = (
        dataframe
        .tumbling_window(duration_ms=timedelta(hours=1), name='daily_aggregations')
        .agg(
            max_actual_pv=Max('actual_pv'),
            mean_actual_pv=Mean('actual_pv'),
            event_count=Count())
        .final()
    )
    # store aggregations in database
    dataframe_hourly_aggregations.sink(postgres_sink_hourly_aggregations)
    if debug: print(dataframe_hourly_aggregations)

    # daily aggregation of max and mean values of actual power production
    dataframe_daily_aggregations = (
        dataframe
        .tumbling_window(duration_ms=timedelta(hours=24), name='daily_aggregations')
        .agg(
            max_actual_pv=Max('actual_pv'),
            mean_actual_pv=Mean('actual_pv'),
            event_count=Count())
        .final()
    )
    # store aggregations in database
    dataframe_daily_aggregations.sink(postgres_sink_daily_aggregations)
    if debug: print(dataframe_daily_aggregations)

    # weekly aggregation of max and mean values of actual power production
    dataframe_weekly_aggregations = (
        dataframe
        .tumbling_window(duration_ms=timedelta(days=7), name='weekly_aggregations')
        .agg(
            max_actual_pv=Max('actual_pv'),
            mean_actual_pv=Mean('actual_pv'),
            event_count=Count())
        .final()
    )
    # store aggregations in database
    dataframe_weekly_aggregations.sink(postgres_sink_weekly_aggregations)
    if debug: print(dataframe_weekly_aggregations)

    # after all dataframe operations are set, the application can start and will track all aggregations in parallel
    app.run()

if __name__ == "__main__":
    # list of the processes to be able to access them
    processes = list()

    # as configured in PROCESS_CONSUMER_COUNT, that many parallel processes will be started to consume data from
    # Kafka topic and process it
    # because all processes will use the same consumer group ID, they easily can share the load, while Kafka takes
    # care not to delive duplicate data
    for consumer_number in range(1, process_consumer_count + 1):
        process = Process(target=process_process_messages,
                          args=(consumer_group_id,
                                kafka_servers,
                                kafka_topic_input,
                                consumer_number,
                                debug))
        # catch process and put it into list of processes
        processes.append(process)
        process.start()
