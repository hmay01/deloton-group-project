from os import getenv
from typing import List, Optional

import confluent_kafka
from datetime import date
import json
import pandas as pd
import numpy as np
import re
import uuid

from dotenv import load_dotenv
from sqlalchemy import create_engine, text



class Kafka_helpers():
    load_dotenv()
    KAFKA_TOPIC_NAME = getenv('KAFKA_TOPIC')
    @staticmethod
    def connect_to_kafka_consumer() -> confluent_kafka.Consumer:
        """ 
        Connect to the server and return a consumer
        """
        
        KAFKA_SERVER = getenv('KAFKA_SERVER')
        KAFKA_USERNAME = getenv('KAFKA_USERNAME')
        KAFKA_PASSWORD = getenv('KAFKA_PASSWORD')


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
        user_dict = Kafka_helpers.get_user_dict(log)
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
    
    @staticmethod
    def get_age(dob:date) -> int:
        """
        Calculates a person's age based on their date of birth
        """
        today = date.today()
        try: 
            birthday = dob.replace(year=today.year)
        except ValueError: # raised when birth date is February 29 and the current year is not a leap year
            birthday = dob.replace(year=today.year, month=dob.month+1, day=1)
        if birthday > today:
            return today.year - dob.year - 1
        else:
            return today.year - dob.year
            
    @staticmethod   
    def reg_extract_ride_duration(log: str):
        '''Parse ride_duration from given log text'''
        search = re.search('(duration = )([0-9]*)', log)
        if search is not None: 
            ride_duration = search.group(2)
            return int(ride_duration)
        else:
            return None
    
    @staticmethod   
    def parse_name_log(log):
        if ' [SYSTEM] data' in log:
            return Kafka_helpers.get_value_from_user_dict(log,"name")

    @staticmethod   
    def parse_age_log(log):
        if ' [SYSTEM] data' in log:
            dob_log_string = Kafka_helpers.get_value_from_user_dict(log, 'date_of_birth')
            dob_timestamp = pd.Timestamp(dob_log_string, unit='ms')
            return Kafka_helpers.get_age(dob_timestamp)

    @staticmethod   
    def parse_gender_log(log):
        if ' [SYSTEM] data' in log:
            return Kafka_helpers.get_value_from_user_dict(log,"gender")   
    

    @staticmethod  
    def parse_ride_log(log):

        if  '[INFO]: Ride' in log:
            return Kafka_helpers.reg_extract_ride_duration(log)

    @staticmethod  
    def parse_telemetry_log(log):
        if  '[INFO]: Telemetry' in log:
            return Kafka_helpers.reg_extract_heart_rate(log)

class transform_data():
    
    @staticmethod
    def get_ride_frequency(df):
        """
        Transform df - find the number of rides over genders for the last 12 hour window
        """

        df = df.loc[:,['gender']]
        df = pd.DataFrame(df.groupby(['gender'])['gender'].count())
        df = df.rename(columns={"gender":"ride_count"})
        df = df.reset_index()
        return df
    
    @staticmethod
    def get_age_bins(df: pd.DataFrame):
        """
        Segments customer age column into age bins
        """
        return pd.cut(df['age'], bins = [0,18,26,39,65,np.inf], labels=["Kids (< 18)","Young Adults (18-25)", "Adults (25-40)", "Middle Age (40-65)", "Seniors (65+)"])
    

    @staticmethod
    def get_age_frequency(df, age_bins):
        """
        Transform df - find the number of rides over ages for the last 12 hour window
        """
        df = df.loc[:,['age']]
        df = pd.DataFrame(df.groupby([age_bins])[['age']].count())
        df = df.rename(columns={"age":"ride_count"})
        df = df.reset_index()
        return df

class SQLConnection():
    load_dotenv()
    db_host = getenv('DB_HOST')
    db_port = getenv('DB_PORT')
    db_user = getenv('DB_USER')
    db_password = getenv('DB_PASSWORD')
    db_name = getenv('DB_NAME')
    group_user = getenv('GROUP_USER')
    group_user_pass = getenv('GROUP_USER_PASS')

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    @staticmethod
    def read_query(query:str) -> Optional[List[str]]:
        '''Executes a query and returns the result'''
        res = None
        with SQLConnection.engine.connect() as con:
            for q in query.split(';'):
                try:
                    res = pd.read_sql_query(q.strip(), con)
                except (TypeError, ValueError):
                    pass
        return res