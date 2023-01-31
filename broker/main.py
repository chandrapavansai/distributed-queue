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
    ...


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


@app.get("/consumer/consume/{topic}")
async def dequeue(topic: str):
    ...


@app.get("/size/{topic}")
def size(topic: str):
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

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    cursor.execute("SELECT COUNT(*) FROM Queue WHERE topic_name = %s", (topic,))
    count = cursor.fetchone()[0]
    print(cursor.fetchone())
    return {"size": count}
