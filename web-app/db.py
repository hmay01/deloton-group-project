import pandas  as pd
import plotly.express as px
from dashboard_helper import transform_data as td,SQLConnection as sql



recent_rides_df = pd.DataFrame(sql.read_query("""
SELECT ride_id, start_time,total_duration, gender, age, total_power_kilojoules FROM yusra_stories_production.rides 
JOIN yusra_stories_production.users 
ON rides.user_id = users.user_id 
WHERE rides.start_time > (NOW() - INTERVAL '12 hour') 
""")).set_index('ride_id').sort_index()



ride_frequency_df = td.get_ride_frequency(recent_rides_df)
age_bins = td.get_age_bins(recent_rides_df)
age_frequency_df = td.get_age_frequency(recent_rides_df, age_bins)

ride_frequency = px.pie(ride_frequency_df , values='ride_count', names='gender')
ride_frequency.update_layout( title = 'Ride frequency by gender over the last 12 hours', legend_title_text='User Gender')
age_distribution = px.bar(age_frequency_df,  x="age", y="ride_count", barmode="group", labels={'ride_count': 'Number of rides', 'age':'Rider age'}, title = ' Age distribution of Deloton rider over the last 12 hours ')

total_power_output = recent_rides_df['total_power_kilojoules'].sum().round(2)
avg_power_output = recent_rides_df['total_power_kilojoules'].mean().round(2)


