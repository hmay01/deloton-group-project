# %pip install flask-sqlalchemy
# %pip install Flask-Cors
# %pip install pyarrow

import json
from datetime import datetime, timedelta

from flask import Flask, jsonify, request

from snowflake_helpers import *

conn = connect_to_snowflake()
cs = conn.cursor()
cs.execute(f"USE SCHEMA {PRODUCTION_SCHEMA}")

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index() -> str:
    return "Welcome to the Deloton Exercise Bikes API!"


@app.route('/daily', methods=['GET'])
def get_rides() -> json:
    search_word = request.args.get('date')
    if search_word == None:
        return #Get all of the rides in the current day
    else:
        #Get all rides for a specific date
        return search_word

@app.route('/ride/<id>', methods=['GET','DELETE'])
def ride_id(id:int) -> json:
    """
    For a given ID string input, returns a different JSON object
    based on the chosen request method
    """
    if (request.method == 'GET'):
        return get_ride_by_id(id)

    elif (request.method == 'DELETE'):

        return delete_by_id(id)

@app.route('/rider/<user_id>', methods=['GET'])
def get_rider_info(user_id:int) -> json:
    """
    Returns a JSON object containing rider information (e.g. name, gender, age, 
    avg. heart rate, number of rides) for a rider with a specific ID string input
    """
    return get_rider_info_by_id(user_id)

@app.route('/rider/<user_id>/rides', methods=['GET'])
def get_all_rides_for_given_user(user_id:int) -> json:
    """
    Returns a JSON object containing all rides for a rider with 
    a specific ID string input
    """
    return get_all_rides_for_rider(user_id)



def get_todays_rides():
    #Get all of the rides in the current day
    return

def get_rides_at_specific_date(date:str):
    #Get all rides for a specific date
    return

def get_ride_by_id(id:int) -> json:
    """
    Returns a json object of a ride for a given ride_id
    """
    ride_by_id_df = cs.execute(f'SELECT * FROM RIDES WHERE "ride_id" = {id};').fetch_pandas_all()
    ride_by_id_json = convert_to_json(ride_by_id_df)
    return   ride_by_id_json

def delete_by_id(id:int) -> str:
    """
    Deletes a ride with a specific ID
    """
    cs.execute(f'DELETE FROM RIDES WHERE "ride_id" = {id};')

    return 'Ride Deleted!', 200

def get_rider_info_by_id(user_id:int) -> json:
    """
    Returns a json object of rider information (name, gender, age ect) and 
    aggregate ride info (avg. heart rate, number of rides) for a given user_id
    """
    rider_info_df = cs.execute(f"""
    WITH aggregate_rides AS (
        SELECT "user_id", COUNT("ride_id") AS "number_of_rides" , ROUND(AVG("avg_heart_rate_bpm")) AS "avg_heart_rate_bpm"
        FROM RIDES
        WHERE "user_id" = {user_id}
        GROUP BY "user_id"
    )
    SELECT  "user_id", "name", "gender", "age", "height_cm", "weight_kg", 
    "address", "email_address", "number_of_rides", "avg_heart_rate_bpm"
    FROM USERS 
    JOIN aggregate_rides
    USING ("user_id");
    """).fetch_pandas_all()
    rider_info_json = convert_to_json(rider_info_df)
    return rider_info_json


def get_all_rides_for_rider(user_id:int) -> json:
    """
    Returns a json object of aggregate ride (avg. heart rate, number of rides) info fo a
    given rider, given a user_id
    """
    rides_df = cs.execute(f'SELECT * FROM RIDES WHERE "user_id" = {user_id};').fetch_pandas_all()
    rides_json = convert_to_json(rides_df)
    return rides_json

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