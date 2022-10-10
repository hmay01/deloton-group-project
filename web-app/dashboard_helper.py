from os import getenv
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


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