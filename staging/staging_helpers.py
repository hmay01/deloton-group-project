import json
import uuid
from os import getenv
from typing import List, Optional

import confluent_kafka
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import boto3
load_dotenv()

class SQLConnection():
    load_dotenv()
    db_host = getenv('DB_HOST')
    db_port = getenv('DB_PORT')
    db_user = getenv('DB_USER')
    db_password = getenv('DB_PASSWORD')
    db_name = getenv('DB_NAME')

    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}', pool_pre_ping=True)

    @staticmethod
    def create_db_schemas(schema_list):
        """ 
        Add the schemas in schema list to the database engine.
        """
        with SQLConnection.engine.connect() as con:
            # Avoiding error: This user depends on this database.
            for schema_name in schema_list:
                con.execute(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')

            con.execute(f"""CREATE USER {SQLConnection.db_user} WITH ENCRYPTED PASSWORD '{SQLConnection.db_password}'""")

            for schema_name in schema_list:
                con.execute(f'DROP SCHEMA IF EXISTS {schema_name}')
                con.execute(f'CREATE SCHEMA {schema_name}')
                con.execute(f"""GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {SQLConnection.db_user};""")
        print(f'Schemas: {schema_list} added to DB')

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
    
    @staticmethod
    def write_df_to_table(df:pd.DataFrame, schema:str, table_name:str, if_exists) -> None:
        '''Writes a DataFrame to a SQL table using the given if_exists argument'''
        df.to_sql(table_name, schema = schema, con=SQLConnection.engine, index=False, if_exists=if_exists)
        if if_exists == 'fail':
            print(f'TABLE {table_name} already exists in {schema}')
        elif if_exists == 'append':
            if df.shape[0] == 1:
                print(f'{df.shape[0]} ROW APPENDED TO {table_name.upper()} in {schema}')
            elif df.shape[0] > 1:
                print(f'{df.shape[0]} ROWS APPENDED TO {table_name.upper()} in {schema}')
        elif if_exists == 'replace':
            print(f'New dataframe with {df.shape[0]} rows added to {schema}')
    
    @staticmethod
    def list_tables(schema) -> list:
        """ 
        Queries the information schema for tables in the given schema and returns existing tables in list
        """
        tables_info = SQLConnection.read_query(f"select * from information_schema.tables where table_schema = '{schema}'")
        return list(tables_info.table_name)
    
 
    @staticmethod
    def drop_table(schema:str, table_name:str) -> None:
        '''Drops a table from SQL schema'''
        with SQLConnection.engine.connect() as con:
            statement = text(f"""DROP TABLE IF EXISTS {schema}.{table_name}""")
            con.execute(statement)
            print(f'{table_name} DROPPED from {schema}')
    
    @staticmethod
    def read_table_to_df(schema:str, table_name:str) -> pd.DataFrame:
        '''Reads a SQL table into a new DataFrame'''
        res = pd.read_sql_table(table_name, con=SQLConnection.engine, schema=schema)
        print(f'SQL READ from {schema}.{table_name}')
        return res


    @staticmethod
    def add_empty_logs_table(staging_schema:str, logs_table:str):
        """ 
        Adds an empty logs table to staging schema so that there are no incomplete rides
        """
        empty_df = pd.DataFrame({'ride_id':pd.Series([],dtype='int64'), 'log':pd.Series([],dtype='object')})
        SQLConnection.write_df_to_table(empty_df, staging_schema, logs_table, 'replace')
        return

    @staticmethod
    def get_latest_ride_logs() -> pd.DataFrame:
        """ 
        Queries the logs table in the staging schema for the logs with the max ride id (latest logs)
        Returns the result as a dataframe
        """
        return SQLConnection.read_query(""" 
                SELECT * 
                FROM yusra_stories_staging.logs
                WHERE ride_id = (SELECT MAX("ride_id") FROM yusra_stories_staging.logs)
                """
                )
    
    @staticmethod
    def user_already_in_table(user_id: int) -> bool:
        """ 
        Queries the user table in the production schema to see if the latest user has already been added
        """
        
        print(f'CHECKING if user_id {user_id} in USERS TABLE...')
        if 'users' in SQLConnection.list_production_tables():
            query_df = SQLConnection.read_query(f"""
                                SELECT * 
                                FROM yusra_stories_production.users
                                WHERE user_id = {user_id}
                            """)
            if query_df.shape[0] >= 1:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def is_empty_table(schema, table):
        df = SQLConnection.read_query(f'select * from {schema}.{table}')
        if df.shape[0] == 0:
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

        load_dotenv()

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
    def stream_topic_for_staging(c:confluent_kafka.Consumer, topic: str, sql, sql_schema, logs_table) -> list:
        """
        Constantly streams logs using the provided kafka consumer and topic

        Appends each log to the ride_logs list
            - When a ride comes to an end (signalled by "beginning of main" log), appends the logs for that ride to the SQL logs table
            - When a new ride begins, it appends the new logs to the newly cleared ride_logs list

        Process is repeated

        """
        c.subscribe([topic])
        print(f'Kafka consumer subscribed to topic: {topic}. Logs will be cached from beginning of next ride.')

        ride_logs = []


        if SQLConnection.is_empty_table(sql_schema,logs_table):
            lost_ride_id = 0
        else:
            lost_ride_id = Kafka.get_previous_ride_id(logs_table)
        
        #until a new ride begins...
        ride_id = lost_ride_id
        
        try:
            while True:
                log = c.poll(1.0)
                if log == None:
                    pass
                else: 
                    value = json.loads(log.value().decode('utf-8'))
                    value_log = value['log'] 

                    if 'new ride' in value_log:
                        ride_id += 1
                        print(f'New ride with id: {ride_id}. Collecting logs...')
                        ride_logs.append(value_log)
                        
                    # end of ride log
                    elif 'beginning of main' in value_log:
                        if ride_id == lost_ride_id:
                            pass
                        else:
                            print('Ride successfully ended. Appending logs to the logs table.')
                            #make a mini df and append to logs
                            number_of_rows = len(ride_logs)
                            series_of_ride_id = [ride_id] * number_of_rows
                            latest_ride_df = pd.DataFrame({'ride_id': series_of_ride_id, 'log':ride_logs})
                            sql.write_df_to_table(latest_ride_df, sql_schema, logs_table, 'append')
                            Notify.production_sns_trigger(number_of_rows)
                            ride_logs.clear()

                    # #mid ride logs
                    else:
                        # if its the lost ride...
                        if ride_id == lost_ride_id:
                            pass
                        else:
                            ride_logs.append(value_log)
        except KeyboardInterrupt:
            pass
        finally:
            c.close()

    
    @staticmethod
    def get_previous_ride_id(logs_table:str) -> int:
        """ 
        Queries the logs table for the max ride id and then returns max + 1
        """
        latest_ride_ids_in_table = SQLConnection.read_query(f'''
            select "ride_id" 
            from yusra_stories_staging.{logs_table}
            where "ride_id" = (SELECT MAX("ride_id") FROM yusra_stories_staging.{logs_table})''')
        latest_ride_id = latest_ride_ids_in_table.loc[0]['ride_id']
        return latest_ride_id


class Notify():

    @staticmethod
    def production_sns_trigger(number_of_rows):
        topic_arn = 'arn:aws:sns:eu-west-2:605126261673:y_stories_stage'
        message = {"Number of rows": number_of_rows}
        sns_client = boto3.client(
            'sns', 
            aws_access_key_id=getenv('ACCESS_KEY_ID'), 
            aws_secret_access_key=getenv('SECRET_ACCESS_KEY'),
            region_name = 'eu-west-2')
        sns_client.publish(
            TopicArn=topic_arn, 
            Message=json.dumps({'default': json.dumps(message)}), 
            MessageStructure='json')
        print(f'SNS message sent to trigger production lambda.')

