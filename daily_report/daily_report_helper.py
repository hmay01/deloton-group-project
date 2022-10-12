import os
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os import getenv

import boto3
import numpy as np
import pandas as pd
import plotly.express as px
import sqlalchemy
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine
from xhtml2pdf import pisa


class Graph():

    load_dotenv()
    
    db_host = getenv('DB_HOST')
    db_port = getenv('DB_PORT')
    db_user = getenv('DB_USER')
    db_password = getenv('DB_PASSWORD')
    db_name = getenv('DB_NAME')

    @staticmethod
    def create_connection() -> sqlalchemy.engine.Connection:
        """
        Creates an SQLAlchemy connection for a specified set of user,
        password, hostname, port and database_name
        """
        engine = create_engine(f'postgresql://{Graph.db_user}:{Graph.db_password}@{Graph.db_host}:{Graph.db_port}/{Graph.db_name}')
        con = engine.connect()
        return con

    @staticmethod
    def create_directory_for_images():
        """
        Creates an image directory if not already created
        """
        if not os.path.exists("images"):
            os.mkdir("images")

    @staticmethod
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

    @staticmethod
    def get_graphs(con: sqlalchemy.engine.Connection) -> list:
        """
        Returns a list of plotly graphs
        """
        riders_gender_split_fig = Graph.get_rider_gender_split_fig(con)
        ages_of_riders_fig = Graph.get_age_of_riders_fig(con)
        riders_average_power_and_heart_rate_fig = Graph.get_average_ride_stats_fig(con)
        graphs = [riders_gender_split_fig, ages_of_riders_fig, riders_average_power_and_heart_rate_fig]
        return graphs

    @staticmethod
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

    @staticmethod
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
        groupby_age_df = Graph.group_df_by_age(ages_of_riders)
        ages_of_riders_fig = px.pie(groupby_age_df, values='user_id', names='age', title=f'Age of riders', color_discrete_sequence=px.colors.sequential.Greens_r)
        return ages_of_riders_fig

    @staticmethod
    def group_df_by_age(df: pd.DataFrame):
        """
        Segments age column into age bins and returns a dataframe grouped by age
        """
        age_bins =  pd.cut(df['age'], bins = [0,18,26,39,65,np.inf], labels=["Kids (< 18)","Young Adults (18-25)", "Adults (25-40)", "Middle Age (40-65)", "Seniors (65+)"])
        groupby_age_df = groupby_age_df = df.groupby([age_bins])[['user_id']].count().sort_values(by = 'user_id', ascending = False).reset_index()
        return groupby_age_df

    @staticmethod
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

    @staticmethod
    def get_graph_names() -> list:
        """
        Returns a list of figure names
        """
        graph_names = ['riders_gender_split_fig', 'ages_of_riders_fig', 'riders_average_power_and_heart_rate_fig']
        return graph_names




class Convert():

    @staticmethod
    def output_graphs_to_png(graphs: list, graph_names: list):
        """
        Given a list of graphs and graph name
        Performs parallel itteration to save each graph as a png given its 
        corresponding graph name
        """
        for graph, graph_name in zip(graphs, graph_names):
                Convert.save_graph_as_png(graph, graph_name)
            
    
    @staticmethod       
    def save_graph_as_png(fig, fig_name: str):
        """
        Converts a plotly graph input to a png image stored in the images directory
        """
        fig.write_image(f"images/{fig_name}.png")


    @staticmethod 
    def graph_block_template(file_name: str) -> str:
        """
        Creates an html string for an image insert for a given figure name
        """
        graph_block =  (''
                
                    f'<center><img style="height: 400px;" src="images/{file_name}.png"></center>'
                    + '<hr>'
            )                   
    
        return graph_block

    @staticmethod 
    def get_report(graph_names: list, number_of_rides : np.int64) -> str:
        """
        Returns a html string of the report layout containing the graph 
        image inserts for the input list of graph names 
        """
        graphs_layout = ''
        for graph_name in graph_names:
            graphs_layout += Convert.graph_block_template(graph_name)
        report_layout = (
            '<h1 align="center"> Deloton Exercise Bikes Daily Report</h1>'
            + '<hr>'
            + f'<h1 align="center"> {number_of_rides} Rides completed today </h1>'
            + '<hr>'
            + graphs_layout  
        )
        return report_layout

    @staticmethod 
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



class Email():
    SENDER = 'trainee.john.andemeskel@sigmalabs.co.uk'
    RECIPIENT = 'bicycle-ceo@sigmalabs.co.uk'
    SUBJECT = 'Deloton Exercise Bikes Daily Report'
    BODY_HTML = """<html>
        <br>Good Afternoon,
        <br>Attached is the Daily report pdf. 
        <br>Best wishes,
        <br>Yusra stories team
        </html>"""

    BODY_TEXT = 'Good Afternoon,\nAttached is the Daily report pdf.\nBest wishes,\nYusra stories team'
    ATTACHMENT = 'report.pdf'
    CHARSET = "utf-8"
    REGION_NAME = 'us-east-1'

    @staticmethod
    def create_multipart_message(
            sender: str, recipient: str, subject: str, html: str=None, text: str=None, attachment: str=None)\
            -> MIMEMultipart:
        """
        Creates a MIME multipart message object.
        Emails, both sender and recipients
        """
        multipart_content_subtype = 'alternative' if html else 'mixed'
        msg = MIMEMultipart(multipart_content_subtype)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient

        msg_body = MIMEMultipart('alternative')

        textpart = MIMEText(text.encode(Email.CHARSET), 'plain', Email.CHARSET)
        htmlpart = MIMEText(html.encode(Email.CHARSET), 'html', Email.CHARSET)

        msg_body.attach(textpart)
        msg_body.attach(htmlpart)

        att = MIMEApplication(open(attachment, 'rb').read())

        att.add_header('Content-Disposition','attachment',filename=os.path.basename(attachment))

        msg.attach(msg_body)
        msg.attach(att)
        return msg

    @staticmethod
    def send_mail() -> dict:
        """
        Send email to recipients. Sends one mail to all recipients.
        The sender needs to be a verified email in SES.
        """
        msg = Email.create_multipart_message(Email.SENDER, Email.RECIPIENT, Email.SUBJECT, Email.BODY_HTML, Email.BODY_TEXT, Email.ATTACHMENT)
        ses_client = boto3.client('ses', 
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name= Email.REGION_NAME,)
        return ses_client.send_raw_email(
            Source=Email.SENDER,
            Destinations=[Email.RECIPIENT],
            RawMessage={'Data': msg.as_string()}
        )
    @staticmethod
    def send_report():
        """
        Function call to execute send mail function in a try block
        Sends daily report email to recipient fixed recipient
        """
        try:
            response = Email.send_mail()
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])