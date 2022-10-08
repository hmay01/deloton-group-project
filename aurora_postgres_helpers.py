from os import getenv
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text


#sql wrapper
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

    def create_db_schemas(self, schema_list):
        """ 
        Add the schemas in schema list to the database engine.
        """
        with self.engine.connect() as con:
            # Avoiding error: This user depends on this database.
            for schema_name in schema_list:
                con.execute(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')

            con.execute(f"""CREATE USER {self.group_user} WITH ENCRYPTED PASSWORD '{self.group_user_pass}'""")

            for schema_name in schema_list:
                con.execute(f'DROP SCHEMA IF EXISTS {schema_name}')
                con.execute(f'CREATE SCHEMA {schema_name}')
                con.execute(f"""GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {self.group_user};""")
        print(f'Schemas: {schema_list} added to DB')


    def read_query(self, query:str) -> Optional[List[str]]:
        '''Executes a query and returns the result'''
        res = None
        with self.engine.connect() as con:
            for q in query.split(';'):
                try:
                    res = pd.read_sql_query(q.strip(), con)
                except (TypeError, ValueError):
                    pass
        return res
    
    def write_df_to_table(self, df:pd.DataFrame, schema:str, table_name:str, if_exists) -> None:
        '''Writes a DataFrame to a SQL table using the given if_exists argument'''
        df.to_sql(table_name, schema = schema, con=self.engine, index=False, if_exists=if_exists)
        if if_exists == 'fail':
            print(f'TABLE {table_name} already exists in {schema}')
        elif if_exists == 'append':
            if df.shape[0] == 1:
                print(f'{df.shape[0]} ROW APPENDED TO {table_name.upper()} in {schema}')
            elif df.shape[0] > 1:
                print(f'{df.shape[0]} ROWS APPENDED TO {table_name.upper()} in {schema}')
        elif if_exists == 'replace':
            print(f'New dataframe with {df.shape[0]} rows added to {schema}')
    
    def list_production_tables(self) -> list:
        """ 
        Queries the information schema for tables in the production schema and returns existing tables in list
        """
        tables_info = self.read_query(self, "select * from information_schema.tables where table_schema = 'yusra_stories_production'")
        return list(tables_info.table_name)
 

    def drop_table(self, schema:str, table_name:str) -> None:
        '''Drops a table from SQL schema'''
        with self.engine.connect() as con:
            statement = text(f"""DROP TABLE IF EXISTS {schema}.{table_name}""")
            con.execute(statement)
            print(f'{table_name} DROPPED from {schema}')
    
    def read_table_to_df(self, schema:str, table_name:str) -> pd.DataFrame:
        '''Reads a SQL table into a new DataFrame'''
        res = pd.read_sql_table(table_name, con=self.engine, schema=schema)
        print(f'SQL READ from {schema}.{table_name}')
        return res



def add_empty_logs_table(sql:SQLConnection, staging_schema:str, logs_table:str):
    """ 
    Adds an empty logs table to staging schema so that there are no incomplete rides
    """
    empty_df = pd.DataFrame({'ride_id':pd.Series([],dtype='int64'), 'log':pd.Series([],dtype='object')})
    sql.write_df_to_table(empty_df, staging_schema, logs_table, 'replace')
    return

def get_latest_ride_logs(sql: SQLConnection) -> pd.DataFrame:
    """ 
    Queries the logs table in the staging schema for the logs with the max ride id (latest logs)
    Returns the result as a dataframe
    """
    return sql.read_query(sql, """ 
            SELECT * 
            FROM yusra_stories_staging.logs
            WHERE ride_id = (SELECT MAX("ride_id") FROM yusra_stories_staging.logs)
            """
            )

def user_already_in_table(sql, user_id: int) -> bool:
    """ 
    Queries the user table in the production schema to see if the latest user has already been added
    """
    
    print(f'CHECKING if user_id {user_id} in USERS TABLE...')
    if 'users' in sql.list_production_tables(sql):
        query_df = sql.read_query(sql, f"""
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

