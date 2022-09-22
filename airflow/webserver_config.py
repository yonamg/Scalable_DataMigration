"""Default configuration for the Airflow webserver"""
import os

from airflow.www.fab_security.manager import 
basedir = os.path.abspath(os.path.dirname(__file__))

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
AUTH_TYPE = AUTH_DB
