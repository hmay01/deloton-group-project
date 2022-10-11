import warnings

from aurora_postgres_helpers import SQLConnection, add_empty_logs_table
from kafka_helpers import (KAFKA_TOPIC_NAME, connect_to_kafka_consumer,
                           stream_ingestion_kafka_topic)

warnings.simplefilter(action='ignore', category=SyntaxWarning)

if __name__ == "__main__":
    sql = SQLConnection()
    staging_schema = 'yusra_stories_staging'
    logs_table = 'logs'

    add_empty_logs_table(sql, staging_schema, logs_table)

    consumer = connect_to_kafka_consumer()
    stream_ingestion_kafka_topic(consumer, KAFKA_TOPIC_NAME, sql, staging_schema, logs_table)

