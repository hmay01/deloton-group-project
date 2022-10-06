# %pip install flask-sqlalchemy
# %pip install Flask-Cors
# %pip install pyarrow

import json
from datetime import datetime, timedelta

from flask import Flask, request

from snowflake_helpers import *

conn = connect_to_snowflake()
cs = conn.cursor()
use_staging_schema(cs)

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index() -> str:
    return "Welcome to the Deloton Exercise Bikes API!"


@app.route('/daily', methods=['GET'])
def get_rides() -> json:
    search_word = request.args.get('date')
    if search_word == None:
        return get_daily_rides()
    else:
        #Get all rides for a specific date
        return search_word

@app.route('/ride/<id>', methods=['GET','DELETE'])
def ride_id(id:str) -> json:
    """
    For a given ID string input, returns a different JSON object
    based on the chosen request method
    """
    if (request.method == 'GET'):
        #Get a ride with a specific ID
        return
    elif (request.method == 'DELETE'):
        #Delete a with a specific ID
        return

@app.route('/rider/<user_id>', methods=['GET'])
def get_rider_info(user_id:str) -> json:
    """
    Returns a JSON object containing rider information (e.g. name, gender, age, 
    avg. heart rate, number of rides) for a rider with a specific ID string input
    """
    return

@app.route('/rider/<user_id>/rides', methods=['GET'])
def get_all_rides_for_given_user(user_id:str) -> json:
    """
    Returns a JSON object containing all rides for a rider with 
    a specific ID string input
    """
    return


def get_daily_rides() -> json:
    """
    Returns a json object of all the rides in the current day
    """
    daily_rides_df = cs.execute("SELECT * FROM DF_TEST_TABLE").fetch_pandas_all()
    daily_rides_json = convert_to_json(daily_rides_df)
    return   daily_rides_json

def convert_to_json(result_set_df:pd.DataFrame) -> json:
    """
    Converts a pandas DataFrame to a JSON object formatted {index: {column : value}}
    """
    result_set_json_string = result_set_df.to_json(orient="index")
    result_set_dict = json.loads(result_set_json_string)
    result_set_json = json.dumps(result_set_dict, indent=4) 
    return result_set_json

def get_variable_date(num_days: int) -> str:
    """
    Returns the date going forward a specified number of days 
    from the current date
    """
    date = str(datetime.now().date() + timedelta(days = num_days))
    return date