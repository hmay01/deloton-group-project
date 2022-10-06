import json
import re
from datetime import date
from datetime import timedelta
import pandas as pd


def get_joined_formatted_df(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Adds all the necessary columns to the df
    """

    # general columns
    df = add_ride_id_column(df)
    df = add_is_new_ride_column(df)
    df = add_is_info_column(df)
    df = add_is_system_column(df)
    df = add_datetime_column(df)
    # user columns (SYSTEM LOGS)
    df = add_user_id_column(df)
    df = add_name_column(df)
    df = add_gender_column(df)
    df = add_date_of_birth_column(df)
    df['date_of_birth'] = df['date_of_birth'].apply(lambda x: pd.Timestamp(x, unit='ms'))
    df = add_age_column(df)
    df = add_height_column(df)
    df = add_weight_column(df)
    df = add_address_column(df) 
    df = add_email_address_column(df)
    df = add_account_create_date_column(df)
    df['account_created'] = df['account_created'].apply(lambda x: pd.Timestamp(x, unit='ms'))
    df = add_bike_serial_column(df)
    df = add_original_source_column(df)
    # ride columns (INFO LOGS)
    df = add_ride_duration_column(df)
    df = add_heart_rate_column(df)
    df = add_resistance_column(df)
    df = add_rpm_column(df)
    df = add_power_column(df)
    return df

def get_users_df(formatted_df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Takes in the joined df for all logs "formatted_df" and returns only those columns relevant for user table
    """
    system_logs = formatted_df[(formatted_df['is_system']) == True]
    unique_user_system_logs = system_logs.drop_duplicates('user_id')
    user_columns = ['user_id', 'name', 'gender', 'date_of_birth', 'age', 'height_cm', 'weight_kg', 'address', 'email_address', 'account_created', 'bike_serial', 'original_source']
    user_df = unique_user_system_logs[user_columns]
    print('Users dataframe ready to be added to snowflake production schema.')
    return user_df


def get_staging_rides_df(formatted_df):
    staging_ride_columns =  ['ride_id', 'user_id', 'time', 'duration_secs', 'heart_rate', 'resistance', 'power', 'rpm']
    staging_rides_df = formatted_df[staging_ride_columns]
    staging_rides_df = add_total_duration_column(staging_rides_df)
    staging_rides_df = add_total_power_column(staging_rides_df)
    staging_rides_df = add_max_heart_rate_column(staging_rides_df)
    staging_rides_df = add_min_heart_rate_column(staging_rides_df)
    staging_rides_df = add_avg_heart_rate_column(staging_rides_df)
    staging_rides_df = add_avg_resistance_column(staging_rides_df)
    staging_rides_df = add_avg_rpm_column(staging_rides_df)
    staging_rides_df = add_start_time_column(staging_rides_df)
    staging_rides_df = add_end_time_column(staging_rides_df)
    return staging_rides_df

def get_final_rides_df(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Takes in the joined df for all logs "formatted_df" and returns only those columns relevant for final rides table
    """
    final_ride_columns = ['ride_id', 'user_id', 'start_time', 'end_time', 'total_duration', 'max_heart_rate_bpm', 'min_heart_rate_bpm', 'avg_heart_rate_bpm', 'avg_resistance', 'avg_rpm', 'total_power_kilojoules']
    rides_df = staging_rides_df[final_ride_columns]
    rides_df = rides_df.drop_duplicates()
    rides_df = rides_df.dropna()
    print('Rides dataframe ready to be added to snowflake production schema.')
    return rides_df


def get_age(dob:date) -> int:
    """
    Calculates a person's age based on their date of birth
    """
    today = date.today()
    try: 
        birthday = dob.replace(year=today.year)
    except ValueError: # raised when birth date is February 29 and the current year is not a leap year
        birthday = dob.replace(year=today.year, month=dob.month+1, day=1)
    if birthday > today:
        return today.year - dob.year - 1
    else:
        return today.year - dob.year


def reg_extract_ride_id(log: str):
    '''Parse ride_id from given log text'''
    search = re.search('^(ride )([0-9]*)', log)
    if search is not None: 
        ride_id = search.group(2)
        return ride_id
    else:
        return None

def reg_extract_log_datetime(log: str):
    '''Parse log datetime from given log text'''
    search = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}', log)
    if search is not None: 
        datetime = search.group(0)
        return datetime
    else:
        return None

def reg_extract_heart_rate(log: str):
    '''Parse heart_rate from given log text'''
    search = re.search('(hrt = )([0-9]*)', log)
    if search is not None: 
        heart_rate = search.group(2)
        return int(heart_rate)
    else:
        return None

def reg_extract_ride_duration(log: str):
    '''Parse ride_duration from given log text'''
    search = re.search('(duration = )([0-9]*)', log)
    if search is not None: 
        ride_duration = search.group(2)
        return int(ride_duration)
    else:
        return None

def reg_extract_resistance(log: str):
    '''Parse resistance from given log text'''
    search = re.search('(resistance = )([0-9]*)', log)
    if search is not None: 
        resistance = search.group(2)
        return int(resistance)
    else:
        return None

def reg_extract_rpm(log: str):
    '''Parse rpm from given log text'''
    search = re.search('(rpm = )([0-9]*)', log)
    if search is not None: 
        rpm = search.group(2)
        return int(rpm)
    else:
        return None

def reg_extract_power(log: str):
    '''Parse power from given log text'''
    search = re.search('(power = )([0-9]*.[0-9]{8})', log)
    if search is not None: 
        power = search.group(2)
        return float(power)
    else:
        return None

def add_is_new_ride_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    To indicate if a log marks the beginning of a new ride
    """
    df['is_new_ride'] = df['log'].str.contains('new ride')
    return df



def add_is_info_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    To indicate if a log is an INFO log 
    """
    df['is_info'] = df.log.str.contains('INFO')
    return df

def add_is_system_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    To indicate if a log is a SYSTEM log 
    """
    df['is_system'] = df.log.str.contains('SYSTEM')
    return df

def add_datetime_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Holds the datetime of when the log was published
    """
    df['time'] = df['log'].apply(reg_extract_log_datetime)
    df['time'] = pd.to_datetime(df['time'])
    return df

def add_age_column(user_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the age of each user
    """
    user_df['age'] = user_df['date_of_birth'].apply(get_age)
    return user_df


def heart_rate_zeros_to_nans(hr):
    if hr is 0:
        return pd.NA
    else:
        return hr


def add_heart_rate_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows heart rate stat for the relevant INFO logs
    """
    df['heart_rate'] = df.log.apply(reg_extract_heart_rate)
    df['heart_rate'] = df['heart_rate'].astype('Int64')
    df['heart_rate'] = df['heart_rate'].apply(heart_rate_zeros_to_nans)
    return df


def add_ride_duration_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows ride duration stat for the relevant INFO logs
    """
    df['duration_secs'] = df['log'].apply(reg_extract_ride_duration)
    return df

def add_resistance_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows resistance setting for the relevant INFO logs
    """
    df['resistance'] = df['log'].apply(reg_extract_resistance)
    return df

def add_rpm_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows rotations per minute for the relevant INFO logs
    """
    df['rpm'] = df['log'].apply(reg_extract_rpm)
    return df

def add_power_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows power stat for the relevant INFO logs
    """
    df['power'] = df['log'].apply(reg_extract_power)
    return df

def get_user_dict(log:str) -> dict:
    """
    Gets the user dictionary from the SYSTEM logs, returns None for other logs
    """
    search = re.search('(data = )({.*})', log)
    if search is not None: 
        user_dict = json.loads(search.group(2))
        return user_dict
    else:
        return None

def get_value_from_user_dict(log:str, value:str) -> pd.Series:
    """
    Gets a given value from the SYSTEM logs user dictionaries, returns None for other logs
    """
    user_dict = get_user_dict(log)
    if user_dict:
        return user_dict[value]
    else:
        return None

def add_ride_id_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Applies the ride_id regex to each log and adds the result into a new column
    """
    df['ride_id'] = df['log'].apply(reg_extract_ride_id)
    return df

def add_user_id_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows user id for the relevant SYSTEM logs
    """
    df['user_id'] = df['log'].apply(get_value_from_user_dict, args=['user_id'])
    df['user_id'] = df['user_id'].astype('Int64')
    return df


def add_name_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows name for the relevant SYSTEM logs
    """
    df['name'] = df['log'].apply(get_value_from_user_dict, args=['name'])
    return df

def add_gender_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows gender for the relevant SYSTEM logs
    """
    df['gender'] = df['log'].apply(get_value_from_user_dict, args=['gender'])
    return df

def add_address_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows address for the relevant SYSTEM logs
    """
    df['address'] = df['log'].apply(get_value_from_user_dict, args=['address'])
    return df

def add_date_of_birth_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows date of birth for the relevant SYSTEM logs
    """
    df['date_of_birth'] = df['log'].apply(get_value_from_user_dict, args=['date_of_birth'])
    return df

def add_email_address_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows email address for the relevant SYSTEM logs
    """
    df['email_address'] = df['log'].apply(get_value_from_user_dict, args=['email_address'])
    return df

def add_height_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows height for the relevant SYSTEM logs
    """
    df['height_cm'] = df['log'].apply(get_value_from_user_dict, args=['height_cm'])
    return df

def add_weight_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows weight for the relevant SYSTEM logs
    """
    df['weight_kg'] = df['log'].apply(get_value_from_user_dict, args=['weight_kg'])
    return df

def add_account_create_date_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows account create date for the relevant SYSTEM logs
    """
    df['account_created'] = df['log'].apply(get_value_from_user_dict, args=['account_create_date'])
    return df

def add_bike_serial_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows bike serial number for the relevant SYSTEM logs
    """
    df['bike_serial'] = df['log'].apply(get_value_from_user_dict, args=['bike_serial'])
    return df

def add_original_source_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows original source for the relevant SYSTEM logs
    """
    df['original_source'] = df['log'].apply(get_value_from_user_dict, args=['original_source'])
    return df


def add_total_duration_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the total duration column to the staging_rides_df df
    """
    staging_rides_df['total_duration'] = staging_rides_df.groupby(by=['ride_id'], dropna=False)['duration_secs'].transform('max')
    staging_rides_df['total_duration'] = staging_rides_df['total_duration'].apply(lambda x: str(timedelta(seconds=x)))
    return staging_rides_df



def add_total_power_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the total power kilojoules column to the staging_rides_df df
    """
    staging_rides_df['total_power_kilojoules'] = staging_rides_df.groupby('ride_id', dropna=False)['power'].transform('sum')
    staging_rides_df['total_power_kilojoules'] = staging_rides_df['total_power_kilojoules'].apply(lambda x: round(x/1000, 2))
    return staging_rides_df


def add_max_heart_rate_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the max heart rate column to the staging_rides_df df
    """
    staging_rides_df['max_heart_rate_bpm'] = staging_rides_df.groupby('ride_id', dropna=False)['heart_rate'].transform('max')
    staging_rides_df['max_heart_rate_bpm'] = staging_rides_df['max_heart_rate_bpm'].apply(lambda x: int(x))
    return staging_rides_df


def add_min_heart_rate_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the min heart rate column to the staging_rides_df df
    """
    staging_rides_df['min_heart_rate_bpm'] = staging_rides_df.groupby('ride_id', dropna=False)['heart_rate'].transform('min')
    staging_rides_df['min_heart_rate_bpm'] = staging_rides_df['min_heart_rate_bpm'].apply(lambda x: int(x))
    return staging_rides_df

def add_avg_heart_rate_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the average heart rate column to the staging_rides_df df
    """
    staging_rides_df['avg_heart_rate_bpm'] = staging_rides_df.groupby('ride_id', dropna=False)['heart_rate'].transform('mean')
    staging_rides_df['avg_heart_rate_bpm'] = staging_rides_df['avg_heart_rate_bpm'].apply(lambda x: int(x))
    return staging_rides_df


def add_avg_resistance_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the average resistance setting column to the staging_rides_df df
    """
    staging_rides_df['avg_resistance'] = staging_rides_df.groupby('ride_id', dropna=False)['resistance'].transform('mean')
    staging_rides_df['avg_resistance'] = staging_rides_df['avg_resistance'].apply(lambda x: int(x))
    return staging_rides_df


def add_avg_rpm_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the average rotations per minute column to the staging_rides_df df
    """
    staging_rides_df['avg_rpm'] = staging_rides_df.groupby('ride_id', dropna=False)['rpm'].transform('mean')
    staging_rides_df['avg_rpm'] = staging_rides_df['avg_rpm'].apply(lambda x: int(x))
    return staging_rides_df


def add_start_time_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the start time column to the staging_rides_df df
    """
    staging_rides_df['start_time'] = staging_rides_df.groupby('ride_id', dropna=False)['time'].transform('min')
    staging_rides_df['start_time'] = staging_rides_df['start_time'].apply(lambda x: x.round(freq='S'))
    return staging_rides_df

def add_end_time_column(staging_rides_df:pd.DataFrame) -> pd.DataFrame:
    """
    Adds the end time column to the staging_rides_df df
    """
    staging_rides_df['end_time'] = staging_rides_df.groupby('ride_id', dropna=False)['time'].transform('max')
    staging_rides_df['end_time'] = staging_rides_df['end_time'].apply(lambda x: x.round(freq='S'))
    return staging_rides_df




