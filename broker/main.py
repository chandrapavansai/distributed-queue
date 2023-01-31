from fastapi import FastAPI, status, HTTPException
import time
from database import db

app = FastAPI()

cursor = db.cursor()
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
    cursor.execute("SELECT COUNT(*) FROM Topic WHERE name = %s", (topic,))
    # Check if topic exists in topic table
    if cursor.rowcount == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Topic not found")

    cursor.execute("SELECT COUNT(*) FROM Queue WHERE topic_name = %s", (topic,))
    count = cursor.fetchone()[0]
    print(cursor.fetchone())
    return {"size": count}
