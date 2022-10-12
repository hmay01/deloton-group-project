
from app_helpers import Functionality as F
from flask import Flask, json, request
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{F.db_user}:{F.db_password}@{F.db_host}:{F.db_port}/{F.db_name}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

@app.route('/', methods=['GET'])
def index() -> str:
    return "Welcome to the Deloton Exercise Bikes API!"


@app.route('/daily', methods=['GET'])
def get_rides() -> json:
    """
    Returns a JSON object of all rides occurring on the date specified
    with the query parameter. If no date is searched, returns a JSON 
    object of all rides on the current date
    """
    searched_date = request.args.get('date')
    if searched_date == None:

        return F.get_todays_rides(db)
    else:
       
        return F.get_rides_at_specific_date(searched_date, db)

@app.route('/ride/<id>', methods=['GET','DELETE'])
def ride_id(id:int) -> json:
    """
    For a given ID string input, returns a different JSON object
    based on the chosen request method
    """
    if (request.method == 'GET'):
        return F.get_ride_by_id(id, db)

    if (request.method == 'DELETE'):

        return F.delete_by_id(id)

@app.route('/rider/<user_id>', methods=['GET'])
def get_rider_info(user_id:int) -> json:
    """
    Returns a JSON object containing rider information (e.g. name, gender, age, 
    avg. heart rate, number of rides) for a rider with a specific ID string input
    """
    return F.get_rider_info_by_id(user_id, db)

@app.route('/rider/<user_id>/rides', methods=['GET'])
def get_all_rides_for_given_user(user_id:int) -> json:
    """
    Returns a JSON object containing all rides for a rider with 
    a specific ID string input
    """
    return F.get_all_rides_for_rider(user_id, db)

if __name__ == "__main__":
    app.run(debug=True)