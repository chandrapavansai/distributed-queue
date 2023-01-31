import psycopg2
from fastapi import FastAPI, status, HTTPException, Request
import time

import crud
from database import db
from uuid import uuid4

app = FastAPI()

# Print PostgreSQL details
print("PostgreSQL server information")
print(db.get_dsn_parameters(), "\n")


@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response


@app.get("/ping")
async def ping():
    return {"message": "pong1"}


# @app.get("/hello/{name}")
# async def say_hello(name: str):
#     return {"message": f"Hello {name}"}

@app.post("/topics/{name}")
def create_topic(name: str):
    try:
        crud.create_topic(name)
    except psycopg2.errors.UniqueViolation:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Topic already exists")
    db.commit()
    return {"message": "Topic created successfully"}


@app.get("/topics")
def list_topics():
    topics = crud.list_topics()
    return {"topics": topics}


@app.post("/consumer/register/{topic}")
async def register_consumer(topic: str):
    """Returns the size of the queue for a given topic.

    Args:
        topic (str): The topic name

    Raises:
        HTTPException: If the topic does not exist

    Returns:{
            "status": success/failure
            on success : "consumer_id": id
            else : "message": error
        }
    """

    cursor = db.cursor()
    cursor.execute("SELECT * FROM Topic WHERE name = %s", (topic,))
    # Check if topic exists in topic table
    if cursor.rowcount is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, message="Topic not found")

    cursor.execute("SELECT COUNT(*) FROM Queue WHERE topic_name = %s", (topic,))
    count = cursor.fetchone()[0]
    return {
        "status": "success",
        "consumer_id": count
    }


@app.post("/producer/register")
async def register_producer(request: Request):
    body = await request.json()
    topic = body["topic"]
    try:
        crud.create_topic(topic)
        db.commit()
    except psycopg2.errors.UniqueViolation:
        db.rollback()

    producer_id = str(uuid4())
    crud.register_producer(producer_id, topic)
    db.commit()
    return {"producer_id": producer_id}


@app.get("/consumer/consume/{topic}")
async def dequeue(topic: str, consumer_id: int):
    """Returns the size of the queue for a given topic.

    Args:
        topic (str): The topic name
        consumer_id (int): id of the current consumer

    Raises:
        HTTPException: If the topic does not exist 

    Returns:{
            "status" : success/failure
            "message" : log message on success/ error on failure
        }
    """

    cursor = db.cursor()
    cursor.execute("SELECT pos FROM Consumer_Topic WHERE consumer_id = %d", (consumer_id,))
    # Check if topic exists in topic table
    if cursor.rowcount is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, message="Topic not found")
    pos = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM Queue WHERE topic_name = %s", (topic,))
    size = cursor.fetchone()[0]
    # check if the topic queue is empty
    if pos == size:
        return {
            "status": "failure",
            "message": "Topic is empty"
        }
    try:
        cursor.execute("UPDATE Consumer_Topic SET pos = pos+1 WHERE consumer_id = %d and topic_name = %s",
                       (consumer_id, topic,))
    except:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to update the position")
    db.commit()
    cursor.execute("""
        SELECT message 
        FROM (SELECT * FROM Queue WHERE topic_name = %s) 
        OFFSET %d ROWS 
        FETCH NEXT 1 ROWS ONLY""",
                   (topic, pos,))
    message = cursor.fetchone()[0]
    return {
        "status": "success",
        "message": message
    }


@app.post("/producer/produce/{topic}")
async def enqueue(topic: str, request: Request):
    if not crud.topic_exists(topic):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    body = await request.json()
    message = body["message"]
    producer_id = body["producer_id"]
    producer_topic = crud.get_producer_topic(producer_id)
    if producer_topic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producer not found")
    if producer_topic != topic:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Producer not registered for this topic")

    crud.enqueue_message(topic, message)
    db.commit()
    return


@app.get("/size/{topic}")
async def size(topic: str, consumer_id : int):
    """Returns the size of the queue for a given topic.

    Args:
        topic (str): The topic name
        consumer_id (int) : consumer's id

    Raises:
        HTTPException: If the topic does not exist

    Returns:
        {
            "size": _size_
        }: The size of the queue for the given topic
    """

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    cursor.execute("SELECT COUNT(*) FROM Consumer_Topic WHERE consumer_id = %d AND topic_name = %s", (consumer_id,topic,))
    count = cursor.fetchone()[0]
    print(cursor.fetchone())
    return {"size": count}
