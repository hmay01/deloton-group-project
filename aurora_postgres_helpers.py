
import sqlalchemy
from sqlalchemy import create_engine

def create_sql_engine(DB_USER:str, DB_PASSWORD:str, DB_HOST:str, DB_PORT:int, DB_NAME:str) -> sqlalchemy.engine.base.Engine:
    """ 
    Create the connection to the postgreSQL database via the Aurora host.
    Returns the resulting engine.
    """
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_string)
    print(f" SQL engine created: {conn_string}:")
    return engine


def create_db_schemas(engine, schema_list, GROUP_USER, GROUP_USER_PASS):
    """ 
    Add the schemas in schema list to the database engine.
    """
    with engine.connect() as con:
        # Avoiding error: This user depends on this database.
        for schema_name in schema_list:
            con.execute(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')

        con.execute(f"""CREATE USER {GROUP_USER} WITH ENCRYPTED PASSWORD '{GROUP_USER_PASS}'""")

        for schema_name in schema_list:
            con.execute(f'DROP SCHEMA IF EXISTS {schema_name}')
            con.execute(f'CREATE SCHEMA {schema_name}')
            con.execute(f"""GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO {GROUP_USER};""")
    print(f'Schemas: {schema_list} added to DB')