from fastapi import FastAPI, HTTPException
from uuid import uuid4
import hashing.hash as HashTable

from api import consumer, producer, topics, heartbeat, managers, broker

app = FastAPI()

app.include_router(consumer.router)
app.include_router(producer.router)
app.include_router(broker.router)
app.include_router(topics.router)
app.include_router(managers.router)
app.include_router(heartbeat.router)


# TODO:
# Run the Heartbeat Algorithm

# Leader Manager
leader_manager = "http://manager2:5000"


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ping")
def ping():
    return {"message": "pong"}
