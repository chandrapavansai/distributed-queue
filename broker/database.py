import os
import psycopg2
from dotenv import load_dotenv

# Take the credentials from the .env file
env_path=os.path.join('/', '.env')
load_dotenv(env_path)
DATABSE_NAME = os.getenv('DB_NAME') if os.getenv('DB_NAME') is not None else 'ds-assgn-1'
USER = os.getenv('DB_USER') if os.getenv('DB_USER') is not None else 'postgres'
PASSWORD = os.getenv('DB_PASSWORD') if os.getenv('DB_PASSWORD') is not None else '1234'
HOST = os.getenv('DB_HOST') if os.getenv('DB_HOST') is not None else 'localhost'
PORT = os.getenv('DB_PORT') if os.getenv('DB_PORT') is not None else '5432'

db = psycopg2.connect(database=DATABSE_NAME,
                      host=HOST,
                      user=USER,
                      password=PASSWORD,
                      port=PORT)
