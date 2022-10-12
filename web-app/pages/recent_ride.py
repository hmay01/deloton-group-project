import dash_bootstrap_components as dbc
import datetime
from dash import dcc, html, register_page

from db import ride_frequency, age_distribution, total_power_output ,avg_power_output 

now = datetime.datetime.now().hour

register_page(__name__)

recent_ride_stats = dbc.Card(
    dbc.CardBody(
        [
            html.H4(children='Recent ride', style = {'text-align':'center'}),
            html.Hr(),
            html.P(children=f'Total power output (kJ): {total_power_output}', style = {'font-weight': 'bold', 'text-align':'center'}),
            html.P(children=f'Average power output (kJ): {avg_power_output}', style = {'font-weight': 'bold', 'text-align':'center'}),
            dcc.Graph(id='graph-output-1',figure=ride_frequency, style = {'font-weight': 'bold'}),
            dcc.Graph(id='graph-output-2',figure=age_distribution, style = {'font-weight': 'bold'})
        ]
    ))

layout = html.Div(children = [recent_ride_stats])



