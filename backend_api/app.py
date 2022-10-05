# %pip install flask-sqlalchemy
# %pip install Flask-Cors
# %pip install pyarrow

from datetime import datetime
from os import getenv

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import ForeignKey, case, desc, func

load_dotenv()

SNOW_USER = getenv('SNOW_USER')
ACCOUNT = getenv('ACCOUNT')
PASSWORD = getenv('PASSWORD')
WAREHOUSE = getenv('WAREHOUSE')
DATABASE = getenv('DATABASE')
SIGMA_SCHEMA = getenv('SIGMA_SCHEMA')
STAGING_SCHEMA = getenv('STAGING_SCHEMA')

app = Flask(__name__)

# app.config['SQLALCHEMY_DATABASE_URI'] = f"snowflake://{SNOW_USER}:{PASSWORD}@{ACCOUNT}/{DATABASE}/{SIGMA_SCHEMA}?warehouse={WAREHOUSE}"
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# CORS(app, origins=["http://127.0.0.1:8080"],  supports_credentials=True)
# db = SQLAlchemy(app)

@app.route('/', methods=['GET'])
def index():
    return "Welcome to the Deloton Exercise Bikes API!"
app.run()