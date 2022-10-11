
from app_helpers import Functionality, json, request

app = Functionality.app 

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

        return Functionality.get_todays_rides()
    else:
       
        return Functionality.get_rides_at_specific_date(searched_date)

@app.route('/ride/<id>', methods=['GET','DELETE'])
def ride_id(id:int) -> json:
    """
    For a given ID string input, returns a different JSON object
    based on the chosen request method
    """
    if (request.method == 'GET'):
        return Functionality.get_ride_by_id(id)

    if (request.method == 'DELETE'):

        return Functionality.delete_by_id(id)

@app.route('/rider/<user_id>', methods=['GET'])
def get_rider_info(user_id:int) -> json:
    """
    Returns a JSON object containing rider information (e.g. name, gender, age, 
    avg. heart rate, number of rides) for a rider with a specific ID string input
    """
    return Functionality.get_rider_info_by_id(user_id)

@app.route('/rider/<user_id>/rides', methods=['GET'])
def get_all_rides_for_given_user(user_id:int) -> json:
    """
    Returns a JSON object containing all rides for a rider with 
    a specific ID string input
    """
    return Functionality.get_all_rides_for_rider(user_id)

