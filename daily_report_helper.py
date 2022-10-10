import os
from base64 import b64encode
from datetime import datetime, timedelta
from os import getenv

import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine
from xhtml2pdf import pisa

load_dotenv()

db_host = getenv('DB_HOST')
db_port = getenv('DB_PORT')
db_user = getenv('DB_USER')
db_password = getenv('DB_PASSWORD')
db_name = getenv('DB_NAME')
group_user = getenv('GROUP_USER')
group_user_pass = getenv('GROUP_USER_PASS')

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

con = engine.connect()

def get_number_of_rides(con):
    query = f"""
    SELECT COUNT(*) AS number_of_rides 
    FROM yusra_stories_production.rides
    WHERE start_time > (NOW() - interval '24 hour');
    """
    number_of_rides = pd.read_sql_query(query, con)
    return number_of_rides._get_value(0,"number_of_rides")

def get_graphs(con) -> list:
    riders_gender_split_fig = get_rider_gender_split_fig(con)
    ages_of_riders_fig = get_age_of_riders_fig(con)
    riders_average_power_and_heart_rate_fig = get_average_ride_stats_fig(con)
    graphs = [riders_gender_split_fig, ages_of_riders_fig, riders_average_power_and_heart_rate_fig]
    return graphs

def get_rider_gender_split_fig(con):
    query = f"""
    WITH riders AS (
    SELECT DISTINCT (user_id), name, gender, age
    FROM yusra_stories_production.users
    JOIN yusra_stories_production.rides
    USING (user_id)
    WHERE start_time > (NOW() - interval '24 hour')
    )
    SELECT gender, COUNT(*) AS number_of_riders
    FROM riders
    GROUP BY gender;
    """
    riders_gender_split = pd.read_sql_query(query, con)
    riders_gender_split_fig = px.pie(riders_gender_split, values='number_of_riders', names='gender', title=f'Gender split of riders of the past day', color_discrete_sequence=px.colors.sequential.Greens_r)
    return riders_gender_split_fig

def get_age_of_riders_fig(con):
    query = f"""
    
    WITH distinct_riders AS (
        SELECT DISTINCT (user_id), age
        FROM yusra_stories_production.users
        JOIN yusra_stories_production.rides
        USING (user_id)
        WHERE start_time > (NOW() - interval '24 hour')
        ORDER BY age ASC
        )
    SELECT age, COUNT(*) AS number_of_riders
    FROM distinct_riders
    GROUP BY age
    ORDER BY age;
    """
    ages_of_riders = pd.read_sql_query(query, con)
    ages_of_riders_fig = px.pie(ages_of_riders, values='number_of_riders', names='age', title=f'Age of riders', color_discrete_sequence=px.colors.sequential.Greens_r)
    return ages_of_riders_fig

def get_average_ride_stats_fig(con):
    query = f"""
    SELECT user_id, ROUND(AVG(avg_heart_rate_bpm)) AS average_heart_rate_bpm, ROUND(AVG(total_power_kilojoules)) AS average_power_KJ
    FROM yusra_stories_production.users
    JOIN yusra_stories_production.rides
    USING (user_id)
    WHERE start_time > (NOW() - interval '24 hour')
    GROUP BY user_id;
    """
    riders_average_power_and_heart_rate = pd.read_sql_query(query, con)
    riders_average_power_and_heart_rate_fig = px.bar(riders_average_power_and_heart_rate, x= 'average_power_kj', y='average_heart_rate_bpm', 
        color_discrete_sequence=px.colors.sequential.Greens_r, 
        labels=dict(average_power_kj ="Average power (KJ)", average_heart_rate_bpm="Average heart rate (bpm"),
        title = 'Average power vs Average heart rate for each rider'
        )
    return riders_average_power_and_heart_rate_fig


def get_graph_names() -> list:
    graph_names = ['riders_gender_split_fig', 'ages_of_riders_fig', 'riders_average_power_and_heart_rate_fig']
    return graph_names

def create_directory_for_images():
    if not os.path.exists("images"):
        os.mkdir("images")

def output_graphs_to_png(graphs, graph_names):
   for graph, graph_name in zip(graphs, graph_names):
            save_graph_as_png(graph, graph_name)
        
def save_graph_as_png(fig, fig_name):
    fig.write_image(f"images/{fig_name}.png")

def graph_block_template(fig_name):

    graph_block =  (''
            
                f'<img style="height: 400px;" src="images/{fig_name}.png">'
                + '<hr>'
           )                   
   
    return graph_block

def get_report(graph_names, number_of_rides):
    graphs_layout = ''
    for graph_name in graph_names:
        graphs_layout += graph_block_template(graph_name)
    report_layout = (
        '<h1>Deloton Exercise Bikes Daily Report</h1>'
        + '<hr>'
        + f'<h1> {number_of_rides} Rides completed today </h1>'
        + '<hr>'
        + graphs_layout
    )
    return report_layout

def convert_html_to_pdf(source_html, output_filename):

    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
            source_html,           
            dest=result_file)           

    result_file.close()           

    return pisa_status.err