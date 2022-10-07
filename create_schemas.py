from os import getenv

from dotenv import load_dotenv

from aurora_postgres_helpers import create_db_schemas, create_sql_engine

load_dotenv()

DB_HOST = getenv('DB_HOST')
DB_PORT = getenv('DB_PORT')
DB_USER = getenv('DB_USER')
DB_PASSWORD = getenv('DB_PASSWORD')
DB_NAME = getenv('DB_NAME')
GROUP_USER = getenv('GROUP_USER')
GROUP_USER_PASS = getenv('GROUP_USER_PASS')


if __name__ == "__main__":
    engine = create_sql_engine(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    schema_list = ['yusra_stories_staging', 'yusra_stories_production']
    create_db_schemas(engine, schema_list, GROUP_USER, GROUP_USER_PASS)