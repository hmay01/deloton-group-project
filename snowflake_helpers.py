from os import getenv
import snowflake.connector

#SNOWFLAKE
SNOW_USER = getenv('SNOW_USER')
ACCOUNT = getenv('ACCOUNT')
PASSWORD = getenv('PASSWORD')
WAREHOUSE = getenv('WAREHOUSE')
DATABASE = getenv('DATABASE')
SIGMA_SCHEMA = getenv('SIGMA_SCHEMA')
STAGING_SCHEMA = getenv('STAGING_SCHEMA')

def connect_to_snowflake() -> snowflake.connector.connection:
    """
    Connecting to the snowflake database
    """
    conn = snowflake.connector.connect(
        user= SNOW_USER,
        account= ACCOUNT,
        password= PASSWORD,
        warehouse= WAREHOUSE,
        database= DATABASE,
    )
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


def use_staging_schema(cs:snowflake.connector.cursor):
    """ 
    Ensure that cursor is using the staging schema
    """
    cs.execute(f"USE SCHEMA {STAGING_SCHEMA}")


def create_logs_table(cs: snowflake.connector.cursor):
    """ 
    Creates the snowflake logs table, which just has a single log message and id per row
    """
    cs.execute(f"""
        CREATE OR REPLACE TABLE logs(
            "log_id" number AUTOINCREMENT,
            "log" STRING)
    """)


def append_logs_to_table(logs: list, cs:snowflake.connector.cursor):
    """ 
    Takes the logs list from the latest ride in the kafka stream and adds them to the snowflake log table
    """
    '''A list of logs is INSERTED INTO the pre-existing logs table'''
    cs.executemany("INSERT INTO logs VALUES(default, %s)", logs)
    print(f'Table updated with {len(logs)} new rows.')

def is_initial_lost_ride(ride_id: int) -> bool:
    return True if ride_id == 0 else False
