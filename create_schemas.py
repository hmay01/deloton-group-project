from aurora_postgres_helpers import SQLConnection

if __name__ == "__main__":
    sql = SQLConnection()
    schema_list = ['yusra_stories_staging', 'yusra_stories_production']
    sql.create_db_schemas(schema_list)