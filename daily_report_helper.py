import os
from base64 import b64encode
from os import getenv

import numpy as np
import pandas as pd
import plotly.express as px
import sqlalchemy
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

def create_connection() -> sqlalchemy.engine.Connection:
    """
    Creates an SQLAlchemy connection for a specified set of user,
    password, hostname, port and database_name
    """
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    con = engine.connect()
    return con

def create_directory_for_images():
    """
    Creates an image directory if not already created
    """
    if not os.path.exists("images"):
        os.mkdir("images")

def get_number_of_rides(con: sqlalchemy.engine.Connection) -> np.int64:
    """
    Returns the number of rides taken in the last 24 hrs
    """
    query = f"""
    SELECT COUNT(*) AS number_of_rides 
    FROM yusra_stories_production.rides
    WHERE start_time > (NOW() - interval '24 hour');
    """
    number_of_rides = pd.read_sql_query(query, con)
    return number_of_rides._get_value(0,"number_of_rides")

def get_graphs(con: sqlalchemy.engine.Connection) -> list:
    """
    Returns a list of plotly graphs
    """
    riders_gender_split_fig = get_rider_gender_split_fig(con)
    ages_of_riders_fig = get_age_of_riders_fig(con)
    riders_average_power_and_heart_rate_fig = get_average_ride_stats_fig(con)
    graphs = [riders_gender_split_fig, ages_of_riders_fig, riders_average_power_and_heart_rate_fig]
    return graphs

def get_rider_gender_split_fig(con: sqlalchemy.engine.Connection) -> px.pie :
    """
    Given an SQLAlchemy connection
    Returns a pie chart of the gender split of riders in the last 24 hrs
    """
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

def get_age_of_riders_fig(con: sqlalchemy.engine.Connection) -> px.pie:
    """
    Given an SQLAlchemy connection
    Returns a pie chart grouping the age of riders in the last 24 hrs
    """
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

def get_average_ride_stats_fig(con: sqlalchemy.engine.Connection) -> px.bar:
    """
    Given an SQLAlchemy connection
    Returns a bar chart of the average power against the average heart rate
    per rider in the last 24 hrs
    """
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
    """
    Returns a list of figure names
    """
    graph_names = ['riders_gender_split_fig', 'ages_of_riders_fig', 'riders_average_power_and_heart_rate_fig']
    return graph_names

def output_graphs_to_png(graphs: list, graph_names: list):
    """
    Given a list of graphs and graph name
    Performs parallel itteration to save each graph as a png given its 
    corresponding graph name
    """
    for graph, graph_name in zip(graphs, graph_names):
            save_graph_as_png(graph, graph_name)
        
def save_graph_as_png(fig: px, fig_name: str):
    """
    Converts a plotly graph input to a png image stored in the images directory
    """
    fig.write_image(f"images/{fig_name}.png")

def graph_block_template(fig_name: str) -> str:
    """
    Creates an html string for an image insert for a given figure name
    """
    graph_block =  (''
            
                f'<img style="height: 400px;" src="images/{fig_name}.png">'
                + '<hr>'
           )                   
   
    return graph_block

def get_report(graph_names: list, number_of_rides: np.int64) -> str:
    """
    Returns a html string of the report layout containing the graph 
    image inserts for the input list of graph names 
    """
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

def convert_html_to_pdf(source_html: str, output_filename: str) -> int:
    """
    Converts the input source html to a pdf file saved as the 
    string output filename
    """
    result_file = open(output_filename, "w+b")

    pisa_status = pisa.CreatePDF(
            source_html,           
            dest=result_file)           

    result_file.close()           

    return pisa_status.err