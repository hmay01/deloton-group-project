
import warnings

#insignificant warnings filtered as they hide important print messages
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=SyntaxWarning)

from production_helpers import SQLConnection as sql
from production_helpers import Transform as t

def handler(event, context):
    staging_schema = 'yusra_stories_staging'
    production_schema = 'yusra_stories_production'
    logs_table = 'logs'

    #general transformations
    latest_logs = sql.get_latest_ride_logs()
    latest_formatted = t.get_joined_formatted_df(latest_logs)

    #rides table
    staging_ride_df = t.get_staging_rides_df(latest_formatted)
    latest_ride_df = t.get_final_rides_df(staging_ride_df)
    sql.write_df_to_table(latest_ride_df, production_schema, 'rides', 'append')

    #users table
    latest_user_df = t.get_users_df(latest_formatted)
    latest_user_id = latest_user_df["user_id"].iloc[0]

    if sql.user_already_in_table(latest_user_id):
        print(f'user_id: {latest_user_id} ALREADY IN USERS TABLE')
    else:
        sql.write_df_to_table(latest_user_df, production_schema, 'users', 'append')


