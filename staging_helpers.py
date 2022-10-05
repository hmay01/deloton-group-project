import pandas as pd

def get_raw_logs_df(data:list) -> pd.DataFrame:
    """ 
    Makes the initial df which has a row for each log
    """
    return pd.DataFrame(columns=['log'], data=data)