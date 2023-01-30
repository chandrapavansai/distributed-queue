from fastapi import FastAPI,status,HTTPException
import time
import psycopg2
from dotenv import load_dotenv
import os
app = FastAPI()

# Take the credentials from the .env file
env_path=os.path.join('/', '.env')
load_dotenv(env_path)
DATABSE_NAME = os.getenv('DATABSE_NAME') if os.getenv('DATABSE_NAME') is not None else 'ds-assgn-1'
USER = os.getenv('USER') if os.getenv('USER') is not None else 'postgres'
PASSWORD = os.getenv('PASSWORD') if os.getenv('PASSWORD') is not None else '1234'
HOST = os.getenv('HOST') if os.getenv('HOST') is not None else 'localhost'
PORT = os.getenv('PORT') if os.getenv('PORT') is not None else '5432'

conn = psycopg2.connect(database=DATABSE_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)

cursor = conn.cursor()

@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response

queues = {}

@app.get("/ping")
async def ping():
    return {"message": "pong1"}


# @app.get("/hello/{name}")
# async def say_hello(name: str):
#     return {"message": f"Hello {name}"}

@app.post("/topics/{name}")
async def create_topic(name: str):
    ...


@app.get("/topics")
async def list_topics():
    ...


@app.post("/consumer/register/{topic}")
async def register_consumer(topic: str):
    ...


@app.post("/producer/register/{topic}")
async def register_producer(topic: str):
    ...


@app.post("/producer/produce/{topic}")
async def enqueue(topic: str):
    ...


@app.get("/consumer/consume/{topic}")
async def dequeue(topic: str):
    ...


@app.get("/size/{topic}")
async def size(topic: str):
    """Returns the size of the queue for a given topic.

    Args:
        topic (str): The topic name

    Raises:
        HTTPException: If the topic does not exist

    Returns:
        {
            "size": _size_
        }: The size of the queue for the given topic
    """

    cursor.execute("SELECT COUNT(*) FROM topic WHERE name = %s", (topic,))
    # Check if topic exists in topic table
    if cursor.rowcount is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    cursor.execute("SELECT COUNT(*) FROM queue WHERE topic_name = %s", (topic,))
    count = cursor.fetchone()[0]
    print(cursor.fetchone())
    return {"size": count}
    


