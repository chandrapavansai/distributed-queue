import os

import psycopg2


# from dotenv import load_dotenv, find_dotenv

# Take the credentials from the .env file
# env_path = find_dotenv()
# load_dotenv(env_path)


DATABASE_NAME = os.getenv('DB_NAME') if os.getenv(
    'DB_NAME') is not None else 'ds-assgn-2-mgr'
USER = os.getenv('DB_USER') if os.getenv('DB_USER') is not None else 'postgres'
PASSWORD = os.getenv('DB_PASSWORD') if os.getenv(
    'DB_PASSWORD') is not None else 'postgres'
HOST = os.getenv('DB_HOST') if os.getenv(
    'DB_HOST') is not None else 'localhost'
PORT = os.getenv('DB_PORT') if os.getenv('DB_PORT') is not None else '5432'

db = psycopg2.connect(database=DATABASE_NAME,
                      host=HOST,
                      user=USER,
                      password=PASSWORD,
                      port=PORT)


def init_db():
    with db.cursor() as cursor:
        cursor.execute(open("database.sql", "r").read())
        db.commit()


def clear_db():
    with db.cursor() as cursor:
        cursor.execute("DELETE FROM Consumer")
        cursor.execute("DELETE FROM ConsumerPartition")
        cursor.execute("DELETE FROM Producer")
        cursor.execute("DELETE FROM Topic")
        cursor.execute("DELETE FROM Broker")
        cursor.execute("DELETE FROM Manager")
        db.commit()


init_db()
