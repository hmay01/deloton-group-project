import warnings

from pandas.core.common import SettingWithCopyWarning

from aurora_postgres_helpers import SQLConnection
from transformation_helpers import *

#insignificant warnings filtered as they hide important print messages
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=SyntaxWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

if __name__ == "__main__":
    sql = SQLConnection
    staging_schema = 'yusra_stories_staging'
    production_schema = 'yusra_stories_production'
    logs_table = 'logs'

    all_logs_df = sql.read_table_to_df(sql, staging_schema, logs_table)

    latest_ride_id = all_logs_df['ride_id'].max()
    latest_ride_logs = all_logs_df[(all_logs_df['ride_id'] == latest_ride_id)]
    latest_ride_formatted = get_joined_formatted_df(latest_ride_logs)

    latest_user_df = get_users_df(latest_ride_formatted)

    staging_ride_df = get_staging_rides_df(latest_ride_formatted)
    latest_ride_df = get_final_rides_df(staging_ride_df)


    sql.write_df_to_table(sql, latest_user_df, production_schema, 'users', 'append')
    sql.write_df_to_table(sql, latest_ride_df, production_schema, 'rides', 'append')


