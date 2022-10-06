from kafka_helpers import (KAFKA_TOPIC_NAME, connect_to_kafka_consumer,
                           stream_kafka_topic)
from snowflake_helpers import (connect_to_snowflake, create_logs_table,
                               create_staging_schema, use_staging_schema)

from os import getenv

if __name__ == "__main__":
    print(getenv('SNOW_USER'))
    # conn = connect_to_snowflake()
    # cs = conn.cursor()

    # create_staging_schema(cs)
    # use_staging_schema(cs)

    # create_logs_table(cs)

    # consumer = connect_to_kafka_consumer()
    # stream_kafka_topic(consumer, KAFKA_TOPIC_NAME, cs)



