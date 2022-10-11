from kafka_helpers import (KAFKA_TOPIC_NAME, connect_to_kafka_consumer,
                           stream_hr_kafka_topic)


consumer = connect_to_kafka_consumer()
stream_hr_kafka_topic(consumer, KAFKA_TOPIC_NAME)

