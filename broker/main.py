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
    return {"message": "pong"}


@app.post("/topics")
def create_topic(name: str):
    """
    Endpoint to create a topic
    :param name: name of the topic
    :return: success message
    """
    try:
        crud.create_topic(name)
        db.commit()
    except psycopg2.errors.UniqueViolation:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Topic already exists")
    return {"detail": f"Topic {name} created successfully"}


@app.get("/topics")
def list_topics():
    """
    Endpoint to list all topics
    :return: list of topics
    """
    topics = crud.list_topics()
    return {"topics": topics}


@app.post("/consumer/register")
def register_consumer(topic: str):
    """
    Endpoint to register a consumer for a topic
    :param topic: the topic to which the consumer wants to subscribe
    :return: consumer id
    """

    cursor = db.cursor()
    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    consumer_id = str(uuid4())
    crud.register_consumer(consumer_id, topic, cursor)
    db.commit()
    return {"consumer_id": consumer_id}


@app.post("/producer/register")
def register_producer(topic: str):
    """
    Endpoint to register a producer for a topic
    :param topic: the topic to which the producer wants to publish
    :return: producer id
    """
    try:
        crud.create_topic(topic)
        db.commit()
    except psycopg2.errors.UniqueViolation:
        db.rollback()

    producer_id = str(uuid4())
    crud.register_producer(producer_id, topic)
    db.commit()
    return {"producer_id": producer_id}


@app.get("/consumer/consume")
def dequeue(topic: str, consumer_id: str):
    """
    Endpoint to dequeue a message from the queue
    :param topic: the topic from which the consumer wants to dequeue
    :param consumer_id: consumer id obtained while registering
    :return: log message
    """

    cursor = db.cursor()
    # Check if topic exists in topic table
    if crud.topic_exists(topic, cursor) is False:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")
    cursor.execute("SELECT pos FROM Consumer_Topic WHERE consumer_id = %s", (consumer_id,))
    pos = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM Queue WHERE topic_name = %s", (topic,))
    size = cursor.fetchone()[0]
    # check if the topic queue is empty
    if pos == size:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic is empty")
    try:
        cursor.execute("UPDATE Consumer_Topic SET pos = pos+1 WHERE consumer_id = %s",
                       (consumer_id,))
    except:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to update the position")
    db.commit()

    print('hello bro', 'size', size)
    cursor.execute("""
        SELECT message 
        FROM (SELECT * FROM Queue WHERE topic_name = %s) 
        OFFSET %d ROWS 
        FETCH NEXT 1 ROWS ONLY""",
                   (topic, pos,))
    message = cursor.fetchone()[0]
    print(message)
    return {"message": message}


@app.post("/producer/produce")
def enqueue(topic: str, producer_id: str, message: str):
    """
    Endpoint to enqueue a message to the queue
    :param topic: the topic to which the producer wants to enqueue
    :param producer_id: producer id obtained while registering
    :param message: log message to be enqueued
    """
    if not crud.topic_exists(topic):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    producer_topic = crud.get_producer_topic(producer_id)
    if producer_topic is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producer not found")
    if producer_topic != topic:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Producer not registered for this topic")

    crud.enqueue_message(topic, message)
    db.commit()
    return


@app.get("/size")
async def size(topic: str, consumer_id: str):
    """
    Endpoint to get the size of the queue for a given topic
    :param topic: the topic for which the size is to be obtained
    :param consumer_id: consumer id obtained while registering
    :return: size of the queue
    """

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    cursor.execute("SELECT COUNT(*) FROM Consumer_Topic WHERE consumer_id = %s AND topic_name = %s", (consumer_id,topic,))
    count = cursor.fetchone()[0]
    return {"size": count}
