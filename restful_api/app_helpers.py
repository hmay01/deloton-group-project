import json
from datetime import datetime
from os import getenv

from dotenv import load_dotenv
from flask import jsonify



class Functionality():

    load_dotenv()
    
    db_host = getenv('DB_HOST')
    db_port = getenv('DB_PORT')
    db_user = getenv('DB_USER')
    db_password = getenv('DB_PASSWORD')
    db_name = getenv('DB_NAME')
    group_user = getenv('GROUP_USER')
    group_user_pass = getenv('GROUP_USER_PASS')


    @staticmethod
    def get_todays_rides(db) -> json:
        """
        Returns a JSON object of all rides on the current date
        """
        current_date = Utilities.get_current_date()
        todays_rides_result = db.session.execute(f"""
        WITH rides AS (  
        SELECT *, CAST(start_time AS DATE) AS start_date
        FROM yusra_stories_production.rides
        )
        SELECT * 
        FROM rides
        WHERE start_date = '{current_date}'
        ORDER BY ride_id;
        """)
        todays_rides_list = Format.format_rides_as_list(todays_rides_result)
        todays_rides_json = jsonify(todays_rides_list)
        return todays_rides_json

    @staticmethod
    def get_rides_at_specific_date(date:str, db) -> json:
        """
        For a given date string input, 
        Returns a JSON object of the corresponding rides 
        """
        formatted_date = Format.format_date(date)
        rides_at_specified_date_result = db.session.execute(f"""
        WITH rides AS (  
        SELECT *, CAST(start_time AS DATE) AS start_date
        FROM yusra_stories_production.rides
        )
        SELECT * 
        FROM rides
        WHERE start_date = '{formatted_date}'
        ORDER BY ride_id;
        """)
        rides_at_specified_date_list = Format.format_rides_as_list(rides_at_specified_date_result)
        rides_at_specified_date_json = jsonify(rides_at_specified_date_list)
        return rides_at_specified_date_json

    @staticmethod
    def get_ride_by_id(id:int, db) -> json:
        """
        Returns a json object of a ride for a given ride_id
        """
        ride_by_id_result = db.session.execute(f'SELECT * FROM yusra_stories_production.rides WHERE ride_id = {id};')
        ride_by_id_list = Format.format_rides_as_list(ride_by_id_result)
        ride_by_id_json = jsonify(ride_by_id_list)
        return  ride_by_id_json

    @staticmethod
    def delete_by_id(id:int, db) -> str:
        """
        Deletes a ride with a specific ID
        """
        db.session.execute(f'DELETE FROM yusra_stories_production.rides WHERE ride_id = {id};')
        db.session.commit()
    
        return 'Ride Deleted!', 200

    @staticmethod
    def get_rider_info_by_id(user_id:int, db) -> json:
        """
        Returns a json object of rider information (name, gender, age ect) and 
        aggregate ride info (avg. heart rate, number of rides) for a given user_id
        """
        rider_info_result = db.session.execute(f"""
        WITH aggregate_rides AS (
            SELECT "user_id", COUNT("ride_id") AS "number_of_rides" , ROUND(AVG("avg_heart_rate_bpm")) AS "avg_heart_rate_bpm"
            FROM yusra_stories_production.rides
            WHERE "user_id" = {user_id}
            GROUP BY "user_id"
        )
        SELECT  "user_id", "name", "gender", "age", "height_cm", "weight_kg", 
        "address", "email_address", "number_of_rides", "avg_heart_rate_bpm"
        FROM yusra_stories_production.users
        JOIN aggregate_rides
        USING ("user_id");
        """)
        rider_info_list = Format.format_rider_info_as_list(rider_info_result)
        rider_info_json = jsonify(rider_info_list)
        return rider_info_json

    def get_all_rides_for_rider(user_id:int, db) -> json:
        """
        Returns a json object of aggregate ride (avg. heart rate, number of rides) info fo a
        given rider, given a user_id
        """
        rides_for_rider_results = db.session.execute(f'SELECT * FROM yusra_stories_production.rides WHERE "user_id" = {user_id};')
        rides_for_rider_list = Format.format_rides_as_list(rides_for_rider_results)
        rides_for_rider_json = jsonify(rides_for_rider_list)
        return rides_for_rider_json


class Format():

    @staticmethod
    def format_rides_as_list(rides) -> list:
        """
        Returns a list of dicts for a given ride SQLAlchemy cursor result set
        """
        return [Format.format_ride_as_dict(ride) for ride in rides]

    @staticmethod
    def format_rider_info_as_list(riders_info):
        """
        Returns a list of dicts for a given rider info SQLAlchemy cursor result set
        """
        return [Format.format_rider_info_as_dict(rider_info) for rider_info in riders_info]

    @staticmethod
    def format_ride_as_dict(ride) -> dict:
        """
        Formats a ride result from the SQLAlchemy cursor result set as a dict
        """
        return {
            "ride_id": ride.ride_id,
            "user_id": ride.user_id,
            "start_time": ride.start_time,
            "end_time": ride.end_time,
            "total_duration": ride.total_duration,
            "max_heart_rate_bpm": ride.max_heart_rate_bpm,
            "min_heart_rate_bpm": ride.min_heart_rate_bpm,
            "avg_heart_rate_bpm": ride.avg_heart_rate_bpm,
            "avg_resistance": ride.avg_resistance,
            "avg_rpm": ride.avg_rpm,
            "total_power_kilojoules": ride.total_power_kilojoules
        }

    @staticmethod
    def format_rider_info_as_dict(rider_info) -> dict:
        """
        Formats a rider info result from the SQLAlchemy cursor result set as a dict
        """
        return {
            "user_id": rider_info.user_id,
            "name": rider_info.name,
            "gender": rider_info.gender,
            "age": rider_info.age,
            "height_cm": rider_info.height_cm,
            "weight_kg": rider_info.weight_kg,
            "address": rider_info.address,
            "email_address": rider_info.email_address,
            "number_of_rides": rider_info.number_of_rides,
            "avg_heart_rate_bpm": rider_info.avg_heart_rate_bpm
        }

    @staticmethod
    def format_date (searched_date:str) -> str:
        """
        Formats searched date parameter
        """
        date_list = searched_date.split('-')
        day = date_list[0]
        month = date_list[1]
        year = date_list[2]
        formatted_date = year +'-'+ month +'-' + day
        return formatted_date



class Utilities():

    @staticmethod    
    def get_current_date() -> str:
        """
        Returns the date going forward a specified number of days 
        from the current date
        """
        date = str(datetime.now().date())
        return date
