import dash_bootstrap_components as dbc
import datetime
import json
from dash import dcc, html, register_page, Output, Input, callback

from db import ride_frequency, age_distribution, total_power_output ,avg_power_output 
from dashboard_helper import Kafka_helpers as kh

now = datetime.datetime.now().hour

register_page(__name__)

previous_log = {}
consumer = kh.connect_to_kafka_consumer()
consumer.subscribe([kh.KAFKA_TOPIC_NAME])


recent_ride_stats = dbc.Card(
    dbc.CardBody(
        [
            html.H4(children='Recent ride output stats'),
            html.P(children=f'Total power output (kJ): {total_power_output}'),
            html.P(children=f'Average power output (kJ): {avg_power_output}'),
        ]
    )
)


layout = html.Div(
    children = 
    [
        html.Div(id="current_ride"),
        dcc.Interval(id='interval', interval = 1000, n_intervals=0),
        dcc.Graph(id='graph-output-1',figure=ride_frequency),
        dcc.Graph(id='graph-output-2',figure=age_distribution), 
        recent_ride_stats
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

        ride_log = kh.parse_ride_log(log)
        telemetry_log = kh.parse_telemetry_log(log)
        name_log = kh.parse_name_log(log)
        gender_log = kh.parse_gender_log(log)
        age_log = kh.parse_age_log(log)

        if ride_log != None:
            previous_log['ride'] = ride_log
        
        if telemetry_log != None:
            previous_log['telemetry'] = telemetry_log

        if name_log != None:
            previous_log['name'] = name_log
            previous_log['gender'] = gender_log
            previous_log['age'] = age_log
        
        
        current_ride = (
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H4(children='Current rider'),
                        html.P(children=f'Name: {previous_log.get("name", None)}'),
                        html.P(children=f'Gender: {previous_log.get("gender", None)}'),
                        html.P(children=f'Age: {previous_log.get("age", None)}'),
                        html.P(children=f'Ride duration: {previous_log.get("ride", None)}'),
                        html.P(children=f'Average heart rate (bpm): {previous_log.get("telemetry", None)}')
                    ])))

        return current_ride

    