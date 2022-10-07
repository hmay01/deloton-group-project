import json
import uuid
from os import getenv

import confluent_kafka
from dotenv import load_dotenv
import pandas as pd

from snowflake_helpers import append_logs_to_table, is_initial_lost_ride
from transformation_helpers import reg_extract_heart_rate, get_value_from_user_dict, get_age
import hr_alert_helpers as alert

load_dotenv()

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
        'group.id': f'deloton-group-yusra-stories' +str(uuid.uuid1()),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 1000,
        'fetch.wait.max.ms': 6000,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
        'max.poll.interval.ms': '86400000',
        'topic.metadata.refresh.interval.ms': "-1",
        "client.id": 'id-002-005',
    })

    return c


def stream_kafka_topic(c:confluent_kafka.Consumer, topic: str, snowflake_cursor) -> list:
    """
    Constantly streams logs using the provided kafka consumer and topic

    1. Directly queries logs for live section of dashboard

    2. Appends each log to the ride_logs list
        - When a ride comes to an end (signalled by "beginning of main" log), adds the logs for that ride to the snowflake log table
        - When a new ride begins, it appends the new logs to the newly cleared logs list

    Process is repeated

    """
    c.subscribe([topic])
    print(f'Kafka consumer subscribed to topic: {topic}. Logs will be cached from beginning of next ride.')

    ride_logs = []
    ride_id = 0
    try:
        while True:
            log = c.poll(1.0)
            if log == None:
                pass
            else: 
                value = json.loads(log.value().decode('utf-8'))
                value_log = value['log'] 


                # SAVE ROOM FOR CURRENT RIDE DASH CONNECTION
                    #user = 
                    #gender = 
                    #age = 
                    #current_duration = 
                    #latest_heart_rate = 
                    #current_time = 

                # FILLING UP RIDE LOGS AND LOADING INTO STAGING SCHEMA
             
                # new ride log
                if 'new ride' in value_log:
                    ride_id += 1
                    print(f'New ride with id: {ride_id}. Collecting logs...')
                    ride_logs.append(f'ride {ride_id} {value_log}')
                    
                # end of ride log
                elif 'beginning of main' in value_log:
                    if is_initial_lost_ride(ride_id):
                        pass
                    else:
                        print('Ride successfully ended. Appending logs to the logs table.')
                        append_logs_to_table(ride_logs, snowflake_cursor)
                        ride_logs.clear()

                # #mid ride logs
                else:
                    if is_initial_lost_ride(ride_id):
                        pass
                    else:
                        ride_logs.append(f'ride {ride_id} {value_log}')

                  

    except KeyboardInterrupt:
        pass
    finally:
        c.close()


def stream_hr_kafka_topic(c:confluent_kafka.Consumer, topic: str) -> list:
    """
    Constantly streams logs using the provided kafka consumer and topic
    to directly query logs for heart rate alerts
    """
    c.subscribe([topic])
    print(f'Kafka consumer subscribed to topic: {topic}. Logs will be cached from beginning of next ride.')

    age = None
    try:
        while True:
            log = c.poll(1.0)
            if log == None:
                pass
            else: 
                value = json.loads(log.value().decode('utf-8'))
                value_log = value['log']

                if ' [SYSTEM] data' in value_log:
                    dob_log_string = get_value_from_user_dict(value_log, 'date_of_birth')
                    dob_timestamp = pd.Timestamp(dob_log_string, unit='ms')
                    age = get_age(dob_timestamp)
                    recipient = get_value_from_user_dict('email_address')

                if age != None:
                    heart_rate = reg_extract_heart_rate(value_log)
                    if (heart_rate != None) and (alert.is_abnormal(heart_rate, age)):
                        alert.send_alert(recipient)
                
    except KeyboardInterrupt:
        pass
    finally:
        c.close()