import json
import re
import uuid
from datetime import date
from os import getenv

import boto3
import confluent_kafka
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv


class Email():

    sender = "trainee.john.andemeskel@sigmalabs.co.uk"
    aws_region = "eu-west-2"
    subject = "Important Message: Heart rate alert"
    body_text = (" Your heart rate was picked up at an abnormal rhythm, please seek medical attention! ")
           
    charset = "UTF-8"

    @staticmethod
    def build_html_body(age, heart_rate,name):

        lower_boundary = 40
        upper_boundary = 220 - age

        body_html = f"""
        <html>
        <head></head>
        <body align = "center">
            <img src="https://user-images.githubusercontent.com/80219582/195360654-0544dd66-63fb-4547-bc22-c2d3bc8bdddc.png" alt="Deloton Logo" width = 50% height = 50%>
            <h1>Warning!</h1>
            <p> {name} You may need to seek medical attention </p>
            <p> Your heart rate was recorded as {heart_rate} BPM </p> 
            <p> This is out of range for what is regarded as a healthy heart rate for your age & weight; </p>
            <p> Healthy BPM range: {upper_boundary} - {lower_boundary} </p>
        </body>
        </html>  
        """ 
        return body_html     

    @staticmethod
    def create_email(recipient, age, heart_rate,name):
        """
        Builds the email to be sent as a heart-rate alert
        """

        body_html = Email.build_html_body(age, heart_rate, name)

        client = boto3.client('ses',region_name=Email.aws_region)
        response = client.send_email(
                    Destination=
                    {'ToAddresses': [recipient]},
                    Message={
                        'Body': {
                            'Html': {'Charset': Email.charset,'Data': body_html},
                            'Text': {'Charset': Email.charset, 'Data': Email.body_text}
                        },
                        'Subject': {
                            'Charset': Email.charset, 'Data': Email.subject
                            }
                        },
                    Source=Email.sender,
                )
        return response

    @staticmethod
    def send_alert(recipient, age, heart_rate,name ):
        """
        Fires off the email to the rider if abnormal heart rate occurs
        """
        try:
            response = Email.create_email(recipient, age, heart_rate,name)
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])

class HeartRate():

    @staticmethod
    def heart_rate_boundaries(age: int) -> int:
        """
        Calculate heart rate boundaries given age
        """
        max_hr = 220 - age
        resting_hr = 40
        return resting_hr, max_hr

    @staticmethod
    def is_abnormal(heart_rate: int, age:int) -> bool:
        """
        Compare current heart rate to boundaries, to establish abnormal heart rate
        """
        lower_boundary,upper_boundary = HeartRate.heart_rate_boundaries(age)
        if heart_rate > upper_boundary or (heart_rate < lower_boundary and heart_rate > 0):
            return True
        else:
            return False

class Kafka():

    load_dotenv()
    topic_name = getenv('KAFKA_TOPIC')
    server = getenv('KAFKA_SERVER')
    username = getenv('KAFKA_USERNAME')
    password = getenv('KAFKA_PASSWORD')

    @staticmethod
    def connect_to_consumer() -> confluent_kafka.Consumer:
        """ 
        Connect to the server and return a consumer
        """

        c = confluent_kafka.Consumer({
            'bootstrap.servers': Kafka.server,
            'group.id': f'deloton-group-yusra-stories' +str(uuid.uuid1()),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': Kafka.username,
            'sasl.password': Kafka.password,
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



    @staticmethod
    def stream_hr_kafka_topic(c:confluent_kafka.Consumer, topic: str) -> list:
        """
        Constantly streams logs using the provided kafka consumer and topic
        to directly query logs for heart rate alerts
        """
        c.subscribe([topic])
        print(f'Kafka consumer subscribed to topic: {topic}. Logs will be cached from beginning of next ride.')

        age,recipient,name = None, None, None
        try:
            while True:
                log = c.poll(1.0)
                if log == None:
                    pass
                else: 
                    value = json.loads(log.value().decode('utf-8'))
                    value_log = value['log']

                    if ' [SYSTEM] data' in value_log:
                        dob_log_string = Transformations.get_value_from_user_dict(value_log, 'date_of_birth')
                        dob_timestamp = pd.Timestamp(dob_log_string, unit='ms')
                        age = Transformations.get_age(dob_timestamp)
                        recipient = Transformations.get_value_from_user_dict(value_log,'email_address')
                        name = Transformations.get_value_from_user_dict(value_log,'name')
                 
                    
                    if age != None:
                        heart_rate = Transformations.reg_extract_heart_rate(value_log)
                        if (heart_rate != None) and (HeartRate.is_abnormal(heart_rate, age)):
                            Email.send_alert(recipient, age, heart_rate, name)
                    
        except KeyboardInterrupt:
            pass
        finally:
            c.close()
    

class Transformations():

    @staticmethod
    def get_age(dob:date) -> int:
        """Calculates a users age based on their date of birth, taken leap year into account (value error)"""
        today = date.today()
        try: 
            birthday = dob.replace(year=today.year)
        except ValueError: 
            birthday = dob.replace(year=today.year, month=dob.month+1, day=1)
        if birthday > today:
            return today.year - dob.year - 1
        else:
            return today.year - dob.year

    @staticmethod
    def get_user_dict(log:str) -> dict:
        """
        Gets the user dictionary from the SYSTEM logs, returns None for other logs
        """
        search = re.search('(data = )({.*})', log)
        if search is not None: 
            user_dict = json.loads(search.group(2))
            return user_dict
        else:
            return None

    @staticmethod
    def get_value_from_user_dict(log:str, value:str) -> pd.Series:
        """
        Gets a given value from the SYSTEM logs user dictionaries, returns None for other logs
        """
        user_dict = Transformations.get_user_dict(log)
        if user_dict:
            return user_dict[value]
        else:
            return None

    @staticmethod
    def reg_extract_heart_rate(log: str):
        '''Parse heart_rate from given log text'''
        search = re.search('(hrt = )([0-9]*)', log)
        if search is not None: 
            heart_rate = search.group(2)
            return int(heart_rate)
        else:
            return None
