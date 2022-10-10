import dash_bootstrap_components as dbc
import datetime
import plotly.express as px
from dash import dcc, html, register_page, Input, Output, callback

from db import ride_frequency, age_distribution, total_power_output ,avg_power_output 


now = datetime.datetime.now().hour

register_page(__name__)

# Current ride = dbc.Card(
#     dbc.CardBody(
#         [
#             html.H4(children='Current rider'),
#             html.P(children=f'Name: {current_rider["name"]}'),
#             html.P(children=f'Gender: {current_rider["gender"]}'),
#             html.P(children=f'Age: {current_rider["age"]}'),
#             html.P(children=f'Average heart rate (bpm): {current_rider["avg_hrt"]}'),
#             html.P(children=f'Ride duration: {current_rider["duration"]}')
#         ]
#     )
# )

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
        # insight,
        dcc.Graph(id='graph-output-1',figure=ride_frequency),
        dcc.Graph(id='graph-output-2',figure=age_distribution), 
        recent_ride_stats 
])



