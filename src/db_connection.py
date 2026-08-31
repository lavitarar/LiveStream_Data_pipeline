import os 
import psycopg2  #This is Generally use when we want to run query of postgres
from dotenv import load_dotenv
load_dotenv()

def get_db_connection():
    connection =psycopg2.connect(
        host = os.getenv('DB_HOST'),
        port = os.getenv("DB_PORT"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password  = os.getenv("DB_PASSWORD"))

    return connection