import json
import pandas as pd
import re

def init_df(data:list) -> pd.DataFrame:
    """ 
    Makes the initial df which has a row for each log
    """
    return pd.DataFrame(columns=['log'], data=data)

def format_df(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Adds all the necessary columns to the df
    """
    # general columns
    df = add_is_new_ride_column(df)
    df = add_is_info_column(df)
    df = add_is_system_column(df)
    df = add_date_column(df)
    # ride columns (INFO LOGS)
    df = add_heart_rate_column(df)
    df = add_resistance_column(df)
    df = add_rpm_column(df)
    df = add_power_column(df)
    # user columns (SYSTEM LOGS)
    df = add_user_id_column(df)
    df = add_name_column(df)
    df = add_gender_column(df)
    df = add_address_column(df)
    df = add_email_address_column(df)
    df = add_height_column(df)
    df = add_weight_column(df)
    df = add_account_create_date_column(df)
    df = add_bike_serial_column(df)
    df = add_original_source_column(df)

    return df

def reg_extract_date(log: str):
    '''Parse date from given log text'''
    search = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}', log)
    if search is not None: 
        date = search.group(0)
        return date
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
    df['is_new_ride'] = df.log.str.contains('new ride')
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

def add_date_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Holds the datetime of when the log was published
    """
    df['date'] = df.log.apply(reg_extract_date)
    df['date'] = pd.to_datetime(df.date)
    return df

def add_heart_rate_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows heart rate stat for the relevant INFO logs
    """
    df['heart_rate'] = df.log.apply(reg_extract_heart_rate)
    return df

def add_ride_duration_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows ride duration stat for the relevant INFO logs
    """
    df['ride_duration_secs'] = df['log'].apply(reg_extract_ride_duration)
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

def add_user_id_column(df:pd.DataFrame) -> pd.DataFrame:
    """ 
    Shows user id for the relevant SYSTEM logs
    """
    df['user_id'] = df['log'].apply(get_value_from_user_dict, args=['user_id'])
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
    df['account_create_date'] = df['log'].apply(get_value_from_user_dict, args=['account_create_date'])
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

