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
