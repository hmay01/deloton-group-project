# %pip install flask-sqlalchemy
# %pip install Flask-Cors
# %pip install pyarrow

import json
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv
from flask import Flask, jsonify, request

from snowflake_helpers import *

conn = connect_to_snowflake()
cs = conn.cursor()
use_staging_schema(cs)

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return "Welcome to the Deloton Exercise Bikes API!"
app.run()

@app.route('/daily', methods=['GET'])
def get_daily_rides():
    daily_rides_df = cs.execute("SELECT * FROM DF_TEST_TABLE").fetch_pandas_all()
    daily_rides_json_string = daily_rides_df.to_json(orient="index")
    daily_rides_dict = json.loads(daily_rides_json_string)
    daily_rides_json = json.dumps(daily_rides_dict, indent=4) 
    return   daily_rides_json

@app.route('/ride/<id>', methods=['GET','DELETE'])
def ride_id(id):
    if (request.method == 'GET'):
        #Get a ride with a specific ID
        return
    elif (request.method == 'DELETE'):
        #Delete a with a specific ID
        return

@app.route('/rider/<user_id>', methods=['GET'])
def get_rider_info(user_id):
    #Get rider information (e.g. name, gender, age, avg. heart rate, number of rides)
    return

@app.route('/rider/<user_id>/rides', methods=['GET'])
def get_all_rides_for_given_user(user_id):
    #Get all rides for a rider with a specific ID
    return

@app.route('/rider/<user_id>/rides', methods=['GET'])
def get_all_rides_for_given_user(user_id):
    #Get all rides for a rider with a specific ID
    return
