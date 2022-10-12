
import warnings

from aurora_postgres_helpers import (SQLConnection, get_latest_ride_logs,
                                     user_already_in_table)
from transformation_helpers import *

#insignificant warnings filtered as they hide important print messages
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=SyntaxWarning)


def handler(event, context):

    sql = SQLConnection

    production_schema = 'yusra_stories_production'

    #general transformations
    latest_logs = get_latest_ride_logs(sql)
    latest_formatted = get_joined_formatted_df(latest_logs)

    #rides table
    staging_ride_df = get_staging_rides_df(latest_formatted)
    latest_ride_df = get_final_rides_df(staging_ride_df)
    sql.write_df_to_table(sql, latest_ride_df, production_schema, 'rides', 'append')

    #users table
    latest_user_df = get_users_df(latest_formatted)
    latest_user_id = latest_user_df["user_id"].iloc[0]

    if user_already_in_table(sql, latest_user_id):
        print(f'user_id: {latest_user_id} ALREADY IN USERS TABLE')
    else:
        sql.write_df_to_table(sql, latest_user_df, production_schema, 'users', 'append')


