import confluent_kafka
import uuid
from os import getenv
import json

KAFKA_TOPIC_NAME = getenv('KAFKA_TOPIC')
KAFKA_SERVER = getenv('KAFKA_SERVER')
KAFKA_USERNAME = getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = getenv('KAFKA_PASSWORD')

def connect_to_kafka_consumer() -> confluent_kafka.Consumer:
    """ 
    Connect to the server and return a consumer
    """

    c = confluent_kafka.Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': f'deloton-group-three' +str(uuid.uuid1()),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 1000,
        'fetch.wait.max.ms': 6000,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'false',
        'max.poll.interval.ms': '86400000',
        'topic.metadata.refresh.interval.ms': "-1",
        "client.id": 'id-002-005',
    })

    return c


def stream_kafka_topic(c:confluent_kafka.Consumer, topic: str, number_of_logs: int) -> list:
    """
    Streams a predefined number of logs using the provided kafka consumer and topic
    Returns a list of the logs
    """
    c.subscribe([topic])
    data = []
    try:
        while len(data) <= number_of_logs:
            log = c.poll(1.0)
            if log == None:
                data.append('No message available')
            else: 
                key = log.key().decode('utf-8')
                value = json.loads(log.value().decode('utf-8'))
                topic = log.topic()
                data.append(value['log'])
        return data
    except KeyboardInterrupt:
        pass
    finally:
        c.close()