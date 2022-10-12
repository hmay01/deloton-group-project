import json
import re
from datetime import date, timedelta
from os import getenv
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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
    def list_production_tables() -> list:
        """ 
        Queries the information schema for tables in the production schema and returns existing tables in list
        """
        tables_info = SQLConnection.read_query("select * from information_schema.tables where table_schema = 'yusra_stories_production'")
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

class Transform():

    @staticmethod
    def get_joined_formatted_df(logs_df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Adds all the necessary columns to the latest_logs df
        """

        # general columns
        logs_df = Transform.add_is_new_ride_column(logs_df)
        logs_df = Transform.add_is_info_column(logs_df)
        logs_df = Transform.add_is_system_column(logs_df)
        logs_df = Transform.add_datetime_column(logs_df)
        # user columns (SYSTEM LOGS)
        logs_df = Transform.add_user_id_column(logs_df)
        logs_df = Transform.add_name_column(logs_df)
        logs_df = Transform.add_gender_column(logs_df)
        logs_df = Transform.add_date_of_birth_column(logs_df)
        logs_df['date_of_birth'] = logs_df['date_of_birth'].apply(lambda x: pd.Timestamp(x, unit='ms'))
        logs_df = Transform.add_age_column(logs_df)
        logs_df = Transform.add_height_column(logs_df)
        logs_df = Transform.add_weight_column(logs_df)
        logs_df = Transform.add_address_column(logs_df) 
        logs_df = Transform.add_email_address_column(logs_df)
        logs_df = Transform.add_account_create_date_column(logs_df)
        logs_df['account_created'] = logs_df['account_created'].apply(lambda x: pd.Timestamp(x, unit='ms'))
        logs_df = Transform.add_bike_serial_column(logs_df)
        logs_df = Transform.add_original_source_column(logs_df)
        # ride columns (INFO LOGS)
        logs_df = Transform.add_ride_duration_column(logs_df)
        logs_df = Transform.add_heart_rate_column(logs_df)
        logs_df = Transform.add_resistance_column(logs_df)
        logs_df = Transform.add_rpm_column(logs_df)
        logs_df = Transform.add_power_column(logs_df)
        return logs_df

    @staticmethod
    def get_users_df(formatted_df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Takes in the newly formatted (joined) df for latest logs and returns only those columns relevant for user table
        """
        system_logs = formatted_df[(formatted_df['is_system']) == True]
        user_columns = ['user_id', 'name', 'gender', 'date_of_birth', 'age', 'height_cm', 'weight_kg', 'address', 'email_address', 'account_created', 'bike_serial', 'original_source']
        user_df = system_logs[user_columns]
        return user_df

    @staticmethod
    def get_staging_rides_df(formatted_df):
        """ 
        Takes in the formatted df for the latest logs and adds columns which will be used for the rides df
        """
        staging_ride_columns =  ['ride_id', 'user_id', 'time', 'duration_secs', 'heart_rate', 'resistance', 'power', 'rpm']
        staging_rides_df = formatted_df[staging_ride_columns]
        staging_rides_df = Transform.add_total_duration_column(staging_rides_df)
        staging_rides_df = Transform.add_total_power_column(staging_rides_df)
        staging_rides_df = Transform.add_max_heart_rate_column(staging_rides_df)
        staging_rides_df = Transform.add_min_heart_rate_column(staging_rides_df)
        staging_rides_df = Transform.add_avg_heart_rate_column(staging_rides_df)
        staging_rides_df = Transform.add_avg_resistance_column(staging_rides_df)
        staging_rides_df = Transform.add_avg_rpm_column(staging_rides_df)
        staging_rides_df = Transform.add_start_time_column(staging_rides_df)
        staging_rides_df = Transform.add_end_time_column(staging_rides_df)
        return staging_rides_df

    @staticmethod
    def get_final_rides_df(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Takes in the staging rides df for the latest ride and returns only those columns relevant for final rides table
        """
        final_ride_columns = ['ride_id', 'user_id', 'start_time', 'end_time', 'total_duration', 'max_heart_rate_bpm', 'min_heart_rate_bpm', 'avg_heart_rate_bpm', 'avg_resistance', 'avg_rpm', 'total_power_kilojoules']
        rides_df = staging_rides_df[final_ride_columns]
        rides_df = rides_df.drop_duplicates()
        rides_df = rides_df.dropna()
        print(f'RIDE INFO GATHERED for ride_id: {staging_rides_df["ride_id"][0]}')
        return rides_df

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
    def reg_extract_log_datetime(log: str):
        '''Parse log datetime from given log text'''
        search = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}', log)
        if search is not None: 
            datetime = search.group(0)
            return datetime
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
    def reg_extract_ride_duration(log: str):
        '''Parse ride_duration from given log text'''
        search = re.search('(duration = )([0-9]*)', log)
        if search is not None: 
            ride_duration = search.group(2)
            return int(ride_duration)
        else:
            return None

    @staticmethod
    def reg_extract_resistance(log: str):
        '''Parse resistance from given log text'''
        search = re.search('(resistance = )([0-9]*)', log)
        if search is not None: 
            resistance = search.group(2)
            return int(resistance)
        else:
            return None

    @staticmethod
    def reg_extract_rpm(log: str):
        '''Parse rpm from given log text'''
        search = re.search('(rpm = )([0-9]*)', log)
        if search is not None: 
            rpm = search.group(2)
            return int(rpm)
        else:
            return None

    @staticmethod
    def reg_extract_power(log: str):
        '''Parse power from given log text'''
        search = re.search('(power = )([0-9]*.[0-9]{8})', log)
        if search is not None: 
            power = search.group(2)
            return float(power)
        else:
            return None

    @staticmethod
    def add_is_new_ride_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        To indicate if a log marks the beginning of a new ride
        """
        df['is_new_ride'] = df['log'].str.contains('new ride')
        return df

    @staticmethod
    def add_is_info_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        To indicate if a log is an INFO log 
        """
        df['is_info'] = df.log.str.contains('INFO')
        return df

    @staticmethod
    def add_is_system_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        To indicate if a log is a SYSTEM log 
        """
        df['is_system'] = df.log.str.contains('SYSTEM')
        return df

    @staticmethod
    def add_datetime_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Holds the datetime of when the log was published
        """
        df['time'] = df['log'].apply(Transform.reg_extract_log_datetime)
        df['time'] = pd.to_datetime(df['time'])
        return df

    @staticmethod
    def add_age_column(user_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the age of each user
        """
        user_df['age'] = user_df['date_of_birth'].apply(Transform.get_age)
        return user_df

    @staticmethod
    def heart_rate_zeros_to_nans(hr):
        if hr is 0:
            return pd.NA
        else:
            return hr

    @staticmethod
    def add_heart_rate_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows heart rate stat for the relevant INFO logs
        """
        df['heart_rate'] = df.log.apply(Transform.reg_extract_heart_rate)
        df['heart_rate'] = df['heart_rate'].astype('Int64')
        df['heart_rate'] = df['heart_rate'].apply(Transform.heart_rate_zeros_to_nans)
        return df

    @staticmethod
    def add_ride_duration_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows ride duration stat for the relevant INFO logs
        """
        df['duration_secs'] = df['log'].apply(Transform.reg_extract_ride_duration)
        return df

    @staticmethod
    def add_resistance_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows resistance setting for the relevant INFO logs
        """
        df['resistance'] = df['log'].apply(Transform.reg_extract_resistance)
        return df

    @staticmethod
    def add_rpm_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows rotations per minute for the relevant INFO logs
        """
        df['rpm'] = df['log'].apply(Transform.reg_extract_rpm)
        return df

    @staticmethod
    def add_power_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows power stat for the relevant INFO logs
        """
        df['power'] = df['log'].apply(Transform.reg_extract_power)
        return df

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
        user_dict = Transform.get_user_dict(log)
        if user_dict:
            return user_dict[value]
        else:
            return None

    @staticmethod
    def add_user_id_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows user id for the relevant SYSTEM logs
        """
        df['user_id'] = df['log'].apply(Transform.get_value_from_user_dict, args=['user_id'])
        df['user_id'] = df['user_id'].astype('Int64')
        return df

    @staticmethod
    def add_name_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows name for the relevant SYSTEM logs
        """
        df['name'] = df['log'].apply(Transform.get_value_from_user_dict, args=['name'])
        return df

    @staticmethod
    def add_gender_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows gender for the relevant SYSTEM logs
        """
        df['gender'] = df['log'].apply(Transform.get_value_from_user_dict, args=['gender'])
        return df

    @staticmethod
    def add_address_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows address for the relevant SYSTEM logs
        """
        df['address'] = df['log'].apply(Transform.get_value_from_user_dict, args=['address'])
        return df

    @staticmethod
    def add_date_of_birth_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows date of birth for the relevant SYSTEM logs
        """
        df['date_of_birth'] = df['log'].apply(Transform.get_value_from_user_dict, args=['date_of_birth'])
        return df

    @staticmethod
    def add_email_address_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows email address for the relevant SYSTEM logs
        """
        df['email_address'] = df['log'].apply(Transform.get_value_from_user_dict, args=['email_address'])
        return df

    @staticmethod
    def add_height_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows height for the relevant SYSTEM logs
        """
        df['height_cm'] = df['log'].apply(Transform.get_value_from_user_dict, args=['height_cm'])
        return df

    @staticmethod
    def add_weight_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows weight for the relevant SYSTEM logs
        """
        df['weight_kg'] = df['log'].apply(Transform.get_value_from_user_dict, args=['weight_kg'])
        return df

    @staticmethod
    def add_account_create_date_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows account create date for the relevant SYSTEM logs
        """
        df['account_created'] = df['log'].apply(Transform.get_value_from_user_dict, args=['account_create_date'])
        return df

    @staticmethod
    def add_bike_serial_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows bike serial number for the relevant SYSTEM logs
        """
        df['bike_serial'] = df['log'].apply(Transform.get_value_from_user_dict, args=['bike_serial'])
        return df

    @staticmethod
    def add_original_source_column(df:pd.DataFrame) -> pd.DataFrame:
        """ 
        Shows original source for the relevant SYSTEM logs
        """
        df['original_source'] = df['log'].apply(Transform.get_value_from_user_dict, args=['original_source'])
        return df

    @staticmethod
    def get_age(dob:date) -> int:
        """Calculates a users age based on their date of birth"""
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
    def add_total_duration_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the total duration column to the staging_rides_df df
        """
        staging_rides_df['total_duration'] = staging_rides_df.groupby(by=['ride_id'], dropna=False)['duration_secs'].transform('max')
        staging_rides_df['total_duration'] = staging_rides_df['total_duration'].apply(lambda x: str(timedelta(seconds=x)))
        return staging_rides_df

    @staticmethod
    def add_total_power_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the total power kilojoules column to the staging_rides_df df
        """
        staging_rides_df['total_power_kilojoules'] = staging_rides_df.groupby('ride_id', dropna=False)['power'].transform('sum')
        staging_rides_df['total_power_kilojoules'] = staging_rides_df['total_power_kilojoules'].apply(lambda x: round(x/1000, 2))
        return staging_rides_df

    @staticmethod
    def add_max_heart_rate_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the max heart rate column to the staging_rides_df df
        """
        staging_rides_df['max_heart_rate_bpm'] = staging_rides_df.groupby('ride_id', dropna=False)['heart_rate'].transform('max')
        staging_rides_df['max_heart_rate_bpm'] = staging_rides_df['max_heart_rate_bpm'].apply(lambda x: int(x))
        return staging_rides_df

    @staticmethod
    def add_min_heart_rate_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the min heart rate column to the staging_rides_df df
        """
        staging_rides_df['min_heart_rate_bpm'] = staging_rides_df.groupby('ride_id', dropna=False)['heart_rate'].transform('min')
        staging_rides_df['min_heart_rate_bpm'] = staging_rides_df['min_heart_rate_bpm'].apply(lambda x: int(x))
        return staging_rides_df

    @staticmethod
    def add_avg_heart_rate_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the average heart rate column to the staging_rides_df df
        """
        staging_rides_df['avg_heart_rate_bpm'] = staging_rides_df.groupby('ride_id', dropna=False)['heart_rate'].transform('mean')
        staging_rides_df['avg_heart_rate_bpm'] = staging_rides_df['avg_heart_rate_bpm'].apply(lambda x: int(x))
        return staging_rides_df

    @staticmethod
    def add_avg_resistance_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the average resistance setting column to the staging_rides_df df
        """
        staging_rides_df['avg_resistance'] = staging_rides_df.groupby('ride_id', dropna=False)['resistance'].transform('mean')
        staging_rides_df['avg_resistance'] = staging_rides_df['avg_resistance'].apply(lambda x: int(x))
        return staging_rides_df

    @staticmethod
    def add_avg_rpm_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the average rotations per minute column to the staging_rides_df df
        """
        staging_rides_df['avg_rpm'] = staging_rides_df.groupby('ride_id', dropna=False)['rpm'].transform('mean')
        staging_rides_df['avg_rpm'] = staging_rides_df['avg_rpm'].apply(lambda x: int(x))
        return staging_rides_df

    @staticmethod
    def add_start_time_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the start time column to the staging_rides_df df
        """
        staging_rides_df['start_time'] = staging_rides_df.groupby('ride_id', dropna=False)['time'].transform('min')
        staging_rides_df['start_time'] = staging_rides_df['start_time'].apply(lambda x: x.round(freq='S'))
        return staging_rides_df

    @staticmethod
    def add_end_time_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
        """
        Adds the end time column to the staging_rides_df df
        """
        staging_rides_df['end_time'] = staging_rides_df.groupby('ride_id', dropna=False)['time'].transform('max')
        staging_rides_df['end_time'] = staging_rides_df['end_time'].apply(lambda x: x.round(freq='S'))
        return staging_rides_df

