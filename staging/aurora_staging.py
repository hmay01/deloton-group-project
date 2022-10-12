import warnings

from staging_helpers import Kafka as k
from staging_helpers import SQLConnection as sql

warnings.simplefilter(action='ignore', category=SyntaxWarning)

if __name__ == "__main__":
    staging_schema = 'yusra_stories_staging'
    logs_table = 'logs'
    
    if logs_table not in sql.list_tables(staging_schema):
        sql.add_empty_logs_table(staging_schema, logs_table)
    
    consumer = k.connect_to_consumer()
    k.stream_topic_for_staging(consumer, k.topic_name, sql, staging_schema, logs_table)

