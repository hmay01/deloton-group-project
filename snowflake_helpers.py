from os import getenv
import pandas as pd
import snowflake.connector

#SNOWFLAKE
SNOW_USER = getenv('SNOW_USER')
ACCOUNT = getenv('ACCOUNT')
PASSWORD = getenv('PASSWORD')
WAREHOUSE = getenv('WAREHOUSE')
DATABASE = getenv('DATABASE')
SIGMA_SCHEMA = getenv('SIGMA_SCHEMA')
STAGING_SCHEMA = getenv('STAGING_SCHEMA')


def connect_to_snowflake() -> snowflake.connector.cursor:
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
    return conn.cursor()


def show_schemas(cs:snowflake.connector.cursor) -> list:
    """ 
    Check which schemas are in our database
    """
    return cs.execute("SHOW SCHEMAS;").fetchall()


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


def add_test_table(cs:snowflake.connector.cursor):
    """ 
    Adding test tables to schema to make sure we are using staging schema
    """
    cs.execute(
        "CREATE OR REPLACE TABLE test_table(col1 integer, col2 string)")
    cs.execute(
        "INSERT INTO test_table(col1, col2) VALUES(123, 'xyz'), (456, 'zyx')")


def show_tables(cs:snowflake.connector.cursor, schema) -> list:
    """
    Check which tables are in the current schema the given cursor is using
    """
    return cs.execute(f"SHOW TABLES IN {schema}").fetchall()


def fetch_test_data(cs:snowflake.connector.cursor) -> pd.DataFrame:
    """ 
    Test that insert worked and that we can start work with pandas dataframes
    """
    return cs.execute("SELECT * FROM test_table").fetch_pandas_all()