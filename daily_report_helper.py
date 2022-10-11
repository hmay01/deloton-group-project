import os
from base64 import b64encode
from os import getenv

import boto3
import numpy as np
import pandas as pd
import plotly.express as px
import sqlalchemy
from botocore.client import Config
from botocore.exceptions import ClientError
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

SENDER = "trainee.john.andemeskel@sigmalabs.co.uk"
AWS_REGION = "us-east-1"
SUBJECT = "Daily Report"
BODY_TEXT = (" Your heart rate was picked up at an abnormal rhythm, please seek medical attention! ")
# BODY_HTML = """
# <!DOCTYPE html>
# <html>
#   <head></head>
#   <body>
#     <A href="report.pdf">
#   </body>
# </html>  
# """             
CHARSET = "UTF-8"

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

def get_rider_gender_split_fig(con: sqlalchemy.engine.Connection):
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

def get_age_of_riders_fig(con: sqlalchemy.engine.Connection) :
    """
    Given an SQLAlchemy connection
    Returns a pie chart grouping the age of riders in the last 24 hrs
    """
    query = f"""
        SELECT DISTINCT (user_id), age
        FROM yusra_stories_production.users
        JOIN yusra_stories_production.rides
        USING (user_id)
        WHERE start_time > (NOW() - interval '24 hour')
        ORDER BY age ASC;
    """
    ages_of_riders = pd.read_sql_query(query, con)
    groupby_age_df = group_df_by_age(ages_of_riders)
    ages_of_riders_fig = px.pie(groupby_age_df, values='user_id', names='age', title=f'Age of riders', color_discrete_sequence=px.colors.sequential.Greens_r)
    return ages_of_riders_fig

def group_df_by_age(df: pd.DataFrame):
    """
    Segments age column into age bins and returns a dataframe grouped by age
    """
    age_bins =  pd.cut(df['age'], bins = [0,18,26,39,65,np.inf], labels=["Kids (< 18)","Young Adults (18-25)", "Adults (25-40)", "Middle Age (40-65)", "Seniors (65+)"])
    groupby_age_df = groupby_age_df = df.groupby([age_bins])[['user_id']].count().sort_values(by = 'user_id', ascending = False).reset_index()
    return groupby_age_df

def get_average_ride_stats_fig(con: sqlalchemy.engine.Connection) :
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
    riders_average_power_and_heart_rate_fig = px.scatter(riders_average_power_and_heart_rate, x= 'average_power_kj', y='average_heart_rate_bpm', 
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
        
def save_graph_as_png(fig, fig_name: str):
    """
    Converts a plotly graph input to a png image stored in the images directory
    """
    fig.write_image(f"images/{fig_name}.png")

def save_to_bucket(file_name:str):
    s3 = boto3.resource("s3",
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name=os.environ['AWS_DEFAULT_REGION'],
    )
    data = open(f'images/{file_name}.png', 'rb')
    s3.Bucket("yusra-stories-report-images").put_object(Key= f'{file_name}.png', Body=data)
    print(f"save {file_name} to bucket")

def save_all_images_to_bucket(file_names:list):
    for file_name in file_names:
        save_to_bucket(file_name)

def generating_url_for_image(bucket_name, file_name):
    url = boto3.client('s3',
    config=Config(signature_version='s3v4'),
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name='eu-west-2',
    ).generate_presigned_url(
        ClientMethod='get_object', 
        Params={'Bucket': bucket_name, 'Key': f'{file_name}.png'},
        ExpiresIn=604800)
    return url

def get_all_image_urls(file_names):
    urls = [generating_url_for_image('yusra-stories-report-images', file_name) for file_name in file_names]
    return urls

def graph_block_template(url: str) -> str:
    """
    Creates an html string for an image insert for a given figure name
    """
    graph_block =  (''
            
                f'<img style="height: 400px;" src="{url}">'
                + '<hr>'
           )                   
   
    return graph_block

def get_report(urls: list, number_of_rides : np.int64) -> str:
    """
    Returns a html string of the report layout containing the graph 
    image inserts for the input list of graph names 
    """
    graphs_layout = ''
    for url in urls:
        graphs_layout += graph_block_template(url)
    report_layout = (
        '<h1>Deloton Exercise Bikes Daily Report</h1>'
        + '<hr>'
        + f'<h1> {number_of_rides} Rides completed today </h1>'
        + '<hr>'
        + graphs_layout
    )
    return report_layout

# def convert_html_to_pdf(source_html: str, output_filename: str) -> int:
#     """
#     Converts the input source html to a pdf file saved as the 
#     string output filename
#     """
#     result_file = open(output_filename, "w+b")

#     pisa_status = pisa.CreatePDF(
#             source_html,           
#             dest=result_file)           

#     result_file.close()           

#     return pisa_status.err

def create_email(recipient, BODY_HTML):
    """
    Builds the email to be sent as daily report
    """
    client = boto3.client('ses',region_name=AWS_REGION)
    response = client.send_email(
                Destination=
                {'ToAddresses': [recipient]},
                Message={
                    'Body': {
                        'Html': {'Charset': CHARSET,'Data': BODY_HTML},
                        'Text': {'Charset': CHARSET, 'Data': BODY_TEXT}
                    },
                    'Subject': {
                        'Charset': CHARSET, 'Data': SUBJECT
                        }
                    },
                Source=SENDER,
            )
    return response

def send_report(recipient, BODY_HTML):
    """
    Sends daily report email to recipient
    """
    try:
        response = create_email(recipient, BODY_HTML)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])