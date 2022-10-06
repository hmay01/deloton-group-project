from os import getenv

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
#SNOWFLAKE
SNOW_USER = getenv('SNOW_USER')
ACCOUNT = getenv('ACCOUNT')
PASSWORD = getenv('PASSWORD')
WAREHOUSE = getenv('WAREHOUSE')
DATABASE = getenv('DATABASE')
SIGMA_SCHEMA = getenv('SIGMA_SCHEMA')
STAGING_SCHEMA = getenv('STAGING_SCHEMA')
PRODUCTION_SCHEMA = getenv('PRODUCTION_SCHEMA')

def connect_to_snowflake() -> snowflake.connector.connection:
    """
    Connecting to the snowflake database
    """
    
    conn = snowflake.connector.connect(
        user=SNOW_USER,
        account=ACCOUNT,
        password=PASSWORD,
        warehouse=WAREHOUSE,
        database=DATABASE,
    )
    print(f'Connected to Snowflake with user: {SNOW_USER}')
    return conn


def show_schemas(cs:snowflake.connector.cursor) -> list:
    """ 
    Check which schemas are in our database
    """
    return cs.execute("SHOW SCHEMAS;").fetchall()


def show_tables(cs:snowflake.connector.cursor, schema) -> list:
    """
    Check which tables are in the current schema the given cursor is using
    """
    return cs.execute(f"SHOW TABLES IN {schema}").fetchall()


def create_staging_schema(cs:snowflake.connector.cursor):
    """ 
    Adds the staging schema to the database
    """
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}")
    print(f'Schema: {STAGING_SCHEMA} created.')

def create_production_schema(cs:snowflake.connector.cursor):
    """ 
    Adds the production schema to the database
    """
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {PRODUCTION_SCHEMA}")
    print(f'Schema: {PRODUCTION_SCHEMA} created.')

def use_staging_schema(cs:snowflake.connector.cursor):
    """ 
    Ensure that cursor is using the staging schema
    """
    cs.execute(f"USE SCHEMA {STAGING_SCHEMA}")
    print(f'Cursor now using schema: {STAGING_SCHEMA}')

def use_production_schema(cs:snowflake.connector.cursor):
    """ 
    Ensure that cursor is using the production schema
    """
    cs.execute(f"USE SCHEMA {PRODUCTION_SCHEMA}")


def create_logs_table(cs: snowflake.connector.cursor):
    """ 
    Creates the snowflake logs table, which just has a single log message and id per row
    """
    cs.execute(f"""
        CREATE OR REPLACE TABLE logs(
            "log_id" number AUTOINCREMENT,
            "log" STRING)
    """)
    print('Empty logs table created.')


def append_logs_to_table(logs: list, cs:snowflake.connector.cursor):
    """ 
    Takes the logs list from the latest ride in the kafka stream and adds them to the snowflake log table
    """
    '''A list of logs is INSERTED INTO the pre-existing logs table'''
    cs.executemany("INSERT INTO logs VALUES(default, %s)", logs)
    print(f'Table updated with {len(logs)} new rows.')

def is_initial_lost_ride(ride_id: int) -> bool:
    return True if ride_id == 0 else False

def get_logs_table_as_df(cs:snowflake.connector.cursor) -> pd.DataFrame:
    """ 
    fetch_pandas_all on the result of a SELECT * query on the logs table in the staging schema
    """
    df = cs.execute('SELECT * FROM yusra_stories_staging.logs').fetch_pandas_all()
    print('Logs table from staging schema loaded into dataframe.')
    return df


def create_users_table(cs: snowflake.connector.cursor):
    """ 
    Creates the production ready users table
    """
    cs.execute(f"""
            CREATE OR REPLACE TABLE USERS
            ("name" STRING,
            "gender" STRING,
            "date_of_birth" DATETIME,
            "age" FLOAT,
            "height_cm" FLOAT,
            "weight_kg" FLOAT,
            "address" STRING,
            "email_address" STRING,
            "account_created" DATETIME,
            "bike_serial" STRING,
            "original_source" STRING
            )
    """)
    print('Empty users table created.')


def write_pandas_to_users_table(conn:snowflake.connector.connection, user_df:pd.DataFrame):
    """ 
    Writes the latest version of the users dataframe to the USERS table.
    """
    write_pandas(conn, user_df, 'USERS')
    print(f'Dataframe written to production schema table: USERS')



def create_rides_table(cs: snowflake.connector.cursor):
    """ 
    Creates the production ready rides table
    """
    cs.execute(f"""
            CREATE OR REPLACE TABLE RIDES
            ("user_id" INTEGER,
            "start_time" DATETIME,
            "end_time" DATETIME,
            "total_duration" STRING,
            "max_heart_rate_bpm" INTEGER,
            "min_heart_rate_bpm" INTEGER,
            "avg_heart_rate_bpm" INTEGER,
            "avg_resistance" INTEGER,
            "avg_rpm" INTEGER,
            "total_power_kilojoules" FLOAT
            )
    """)
    print('Empty rides table created.')

def write_pandas_to_rides_table(conn:snowflake.connector.connection, rides_df:pd.DataFrame):
    """ 
    Writes the latest version of the rides dataframe to the RIDES table.
    """
    write_pandas(conn, rides_df, 'RIDES')
    print(f'Dataframe written to production schema table: RIDES')
