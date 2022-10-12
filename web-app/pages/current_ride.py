import dash_bootstrap_components as dbc
import json
from dash import dcc, html, register_page, Output, Input, callback
from dashboard_helper import Kafka_helpers as kh



previous_log = {}
consumer = kh.connect_to_kafka_consumer()
consumer.subscribe([kh.KAFKA_TOPIC_NAME])

register_page(__name__)

layout = html.Div(
    children = 
    [
        
        html.Div(id="current_ride"),
        dcc.Interval(id='interval', interval = 1000, n_intervals=0),

])


@callback(
    Output("current_ride", "children"),
    Input("interval", "n_intervals")
)

def update_live_Dashboard(interval):
    value = consumer.poll(1.0)

    if value:
        value_log = json.loads(value.value().decode('utf-8'))
        log = value_log['log'] 
        ride_log, telemetry_log,name_log, gender_log, age_log = kh.parse_logs(log)

        if ride_log != None:
            previous_log['ride'] = ride_log
        
        if telemetry_log != None:
            previous_log['telemetry'] = telemetry_log

        if name_log != None:
            previous_log['name'] = name_log
            previous_log['gender'] = gender_log
            previous_log['age'] = age_log
        
        if  previous_log['telemetry'] > (upper_boundary := (220 - previous_log.get('age', 0))) or (previous_log['telemetry'] < (lower_boundary := 40) and previous_log['telemetry'] > 0):
            hrt_status = 'WARNING HEART RATE AT ABNORMAL LEVEL'
            status = "danger"
        else:
            hrt_status = 'Heart rate status: Okay'
            status = "success"

        ride_info= (
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H4(children='Current rider information', style = {'text-align':'center', 'font-weight': 'bold'}),
                        html.Hr(),
                        html.P(children=f'Name: {previous_log.get("name", None)}', style = {'font-weight': 'bold'}),
                        html.P(children=f'Gender: {previous_log.get("gender", None)}', style = {'font-weight': 'bold'}),
                        html.P(children=f'Age: {previous_log.get("age", None)}', style = {'font-weight': 'bold'}),
                        html.P(children=f'Ride duration (Seconds): {previous_log.get("ride", None)}', style = {'font-weight': 'bold'})
                       
                    ])))
        
        hrt_level = (
            dbc.Card(
                dbc.CardBody(
                    [
                    html.H4(children = 'Heart rate screening', style = {'text-align':'center', 'font-weight': 'bold'}),
                    html.Hr(),
                    html.P(children = hrt_status, style = {'font-weight': 'bold'}),
                    html.P(children=f'Heart rate (BPM): {previous_log.get("telemetry", None)}', style = {'font-weight': 'bold'}),
                    html.P(children=f'Your healthy heart rate is considered to be between {upper_boundary} and {lower_boundary}', style = {'font-weight': 'bold'})
                    ]),color = status))

        current_ride = dbc.CardGroup([ride_info, hrt_level])
        

        return current_ride