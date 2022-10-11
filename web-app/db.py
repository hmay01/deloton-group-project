import pandas  as pd
from dashboard_helper import SQLConnection
import numpy as np
import plotly.express as px


sql = SQLConnection()

recent_rides_df = pd.DataFrame(sql.read_query("""
SELECT ride_id, start_time,total_duration, gender, age, total_power_kilojoules FROM yusra_stories_production.rides 
JOIN yusra_stories_production.users 
ON rides.user_id = users.user_id 
WHERE rides.start_time > (NOW() - INTERVAL '72 hour') 
""")).set_index('ride_id').sort_index()


def get_ride_frequency(df):
    """
    Transform df - find the number of rides over genders for the last 12 hour window
    """

    df = df.loc[:,['gender']]
    df = pd.DataFrame(df.groupby(['gender'])['gender'].count())
    df = df.rename(columns={"gender":"ride_count"})
    df = df.reset_index()
    return df


def get_age_bins(df: pd.DataFrame):
    """
    Segments customer age column into age bins
    """
    return pd.cut(df['age'], bins = [0,18,26,39,65,np.inf], labels=["Kids (< 18)","Young Adults (18-25)", "Adults (25-40)", "Middle Age (40-65)", "Seniors (65+)"])

def get_age_frequency(df, age_bins):
    """
    Transform df - find the number of rides over ages for the last 12 hour window
    """
    df = df.loc[:,['age']]
    df = pd.DataFrame(df.groupby([age_bins])[['age']].count())
    df = df.rename(columns={"age":"ride_count"})
    df = df.reset_index()
    return df



ride_frequency_df = get_ride_frequency(recent_rides_df)
age_bins = get_age_bins(recent_rides_df)
age_frequency_df = get_age_frequency(recent_rides_df, age_bins)

ride_frequency = px.pie(ride_frequency_df , values='ride_count', names='gender')
ride_frequency.update_layout( title = 'Ride frequency by gender over the last 12 hours', legend_title_text='User Gender')
age_distribution = px.bar(age_frequency_df,  x="age", y="ride_count", barmode="group", labels={'ride_count': 'Number of rides', 'age':'Rider age'}, title = ' Age distribution of Deloton rider over the last 12 hours ')

total_power_output = recent_rides_df['total_power_kilojoules'].sum().round(2)
avg_power_output = recent_rides_df['total_power_kilojoules'].mean().round(2)