#!/usr/bin/env python3
# ©2026 Henri Wahl

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

NEEDED_ENVIRONMENT_VARIABLES = [
    'CONSUMER_GROUP_ID',
    'PROCESS_CONSUMER_COUNT',
    'KAFKA_SERVERS',
    'KAFKA_TOPIC_INPUT',
    'DB_USER',
    'DB_PASSWORD',
    'DB_DATABASE'
]

for needed_environment_variable in NEEDED_ENVIRONMENT_VARIABLES:
    if not environ.get(needed_environment_variable):
        exit(f'{needed_environment_variable} environment variable is not set')

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
    instead of Kafka timestamp.
    """
    return value['timestamp_epoch_ms']


def process_process_messages(group_id: str = 'default',
                             kafka_servers: str = 'localhost:9092',
                             kafka_topic_input: str = 'incoming',
                             consumer_number: int = 1,
                             debug: bool = False):

    print(f"Starting consumer process {consumer_number} in group '{group_id}'")

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

    # Initialize PostgreSQLSink
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

    postgres_sink_monthly_aggregations = PostgreSQLSink(
        host='database',
        port=5432,
        dbname=db_database,
        user=db_user,
        password=db_password,
        table_name='monthly_aggregations',
        schema_auto_update=True,
        primary_key_columns=['city', 'timestamp'],
        upsert_on_primary_key=True
    )
    if debug: print(postgres_sink_monthly_aggregations)

    #dataframe = app.dataframe(topic=topic_input).group_by('site_id')
    dataframe = app.dataframe(topic=topic_input).group_by('city')
    if debug: print(dataframe)

    dataframe_daily_aggregations = (
        dataframe
        .tumbling_window(duration_ms=timedelta(hours=24), name='daily_aggregations')
        .agg(
            max_actual_pv=Max('actual_pv'),
            mean_actual_pv=Mean('actual_pv'),
            event_count=Count())
        .final()
    )
    dataframe_daily_aggregations.sink(postgres_sink_daily_aggregations)
    if debug: print(dataframe_daily_aggregations)

    dataframe_weekly_aggregations = (
        dataframe
        .tumbling_window(duration_ms=timedelta(days=7), name='weekly_aggregations')
        .agg(
            max_actual_pv=Max('actual_pv'),
            mean_actual_pv=Mean('actual_pv'),
            event_count=Count())
        .final()
    )
    dataframe_weekly_aggregations.sink(postgres_sink_weekly_aggregations)
    if debug: print(dataframe_weekly_aggregations)

    dataframe_monthly_aggregations = (
        dataframe
        .tumbling_window(duration_ms=timedelta(days=30), name='monthly_aggregations')
        .agg(
            max_actual_pv=Max('actual_pv'),
            mean_actual_pv=Mean('actual_pv'),
            event_count=Count())
        .final()
    )
    dataframe_monthly_aggregations.sink(postgres_sink_monthly_aggregations)
    if debug: print(dataframe_monthly_aggregations)

    app.run()

if __name__ == "__main__":
    processes = list()

    for consumer_number in range(1, process_consumer_count + 1):
        process = Process(target=process_process_messages,
                          args=(consumer_group_id,
                                kafka_servers,
                                kafka_topic_input,
                                consumer_number,
                                debug))
        processes.append(process)
        process.start()
