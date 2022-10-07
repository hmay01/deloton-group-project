import warnings

from pandas.core.common import SettingWithCopyWarning

from transformation_helpers import (get_final_rides_df,
                                    get_joined_formatted_df,
                                    get_staging_rides_df, get_users_df)

#insignificant warnings filtered as they hide important print messages
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=SyntaxWarning)
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


if __name__ == "__main__":
    conn = connect_to_snowflake()
    cs = conn.cursor()

    create_production_schema(cs)
    use_production_schema(cs)

    logs_df = get_logs_table_as_df(cs)
    formatted_df = get_joined_formatted_df(logs_df)

    user_df = get_users_df(formatted_df)

    staging_rides_df = get_staging_rides_df(formatted_df)
    rides_df = get_final_rides_df(staging_rides_df)

    create_users_table(cs)
    write_pandas_to_users_table(conn, user_df)

    create_rides_table(cs)
    write_pandas_to_rides_table(conn, rides_df)

