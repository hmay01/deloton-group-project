from hr_alert_helpers import Kafka as k

if __name__ == "__main__":
    consumer = k.connect_to_consumer()
    k.stream_hr_kafka_topic(consumer, k.topic_name)

