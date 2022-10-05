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
    daily_rides_json = daily_rides_df.to_json(orient="index")
    parsed = json.loads(daily_rides_json)
   
    return  json.dumps(parsed, indent=4)  