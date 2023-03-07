import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from dotenv import load_dotenv, find_dotenv

# Take the credentials from the .env file
env_path = find_dotenv()
load_dotenv(env_path)


DATABASE_NAME = os.getenv('DB_NAME') if os.getenv('DB_NAME') is not None else 'ds-assgn-1'
USER = os.getenv('DB_USER') if os.getenv('DB_USER') is not None else 'postgres'
PASSWORD = os.getenv('DB_PASSWORD') if os.getenv('DB_PASSWORD') is not None else 'postgres'
HOST = os.getenv('DB_HOST') if os.getenv('DB_HOST') is not None else 'localhost'
PORT = os.getenv('DB_PORT') if os.getenv('DB_PORT') is not None else '5433'

SQLALCHEMY_DATABASE_URL = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE_NAME}'

engine = create_engine(SQLALCHEMY_DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
