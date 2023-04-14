import time
import os
import uvicorn
import requests
import argparse
from fastapi import FastAPI, status, Depends, HTTPException, Query,Response
import schemas
from broker import Broker

app = FastAPI()

leader_url = os.getenv("LEADER_URL") # "http://localhost:8000"
broker = None
broker_host = os.getenv("BROKER") # "localhost"


@app.on_event("startup")
async def ping_manager():
    print(leader_url)
    print(broker_host)
    print("leader_url",leader_url,"broker_host",broker_host)   # Create Broker object
    global broker
    broker_url = broker_host + ':' + '9000'

    # ! For testing purposes
    config = {
        'test': {
            1: ['distributed-queue-raft-broker1-1:9000','distributed-queue-raft-broker2-1:9000','distributed-queue-raft-broker3-1:9000'],
        }
    }

    broker = Broker(broker_url, {} ,broker_host)

    broker_url = 'http://'+ broker_host + ':' + '8000'
    try:
        requests.post(f"{leader_url}/broker?url={broker_url}")
        print("Running in manager-connected mode ...")
    except:
        print("Running in detached mode ...")
    return


@app.middleware("http")
async def add_process_time_header(request, call_next):
    print(request.body)
    # Check if the request is a Raft HTTP message
    if request.headers.get("X-Pysyncobj", "").lower() == "true":
        # If it is, return a 200 response without calling the route handler
        print("Received Raft message")
        # Print the Raft message
        print(request.body)
        return Response(status_code=200)
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.get("/messages", response_model=schemas.Message)
def get_message(topic: str, partition: int, offset: int = 0):
    message = broker.get_message(topic, partition, offset)
    result = schemas.Message(id=offset, content=message)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No message found")
    return result


@app.post("/messages",status_code=status.HTTP_201_CREATED)
def post_message(content: str, topic: str, partition: int, partners: list = Query([], alias="partners")):
    print(topic)
    print(partition)
    print(content)
    print(partners)
    return broker.create_message(topic,partition,content,partners)


@app.get("/messages/count")
def get_message_count(topic: str, partition: int, offset: int = 0):
    return broker.get_message_count(topic, partition, offset)

# delete a topic-partition
@app.get("/messages/delete")
def delete_message(topic: str, partition: int):
    return broker.delete_topic(topic, partition)

@app.post("/new")
def add_new(topic: str, partition: int, partners: list = Query([], alias="partners")):
    return broker.create_topic(topic, partition, partners)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the FastAPI server.")
    parser.add_argument("--port", type=int, default=8000, help="Port number to start the server on.")
    args = parser.parse_args()
    port = args.port
    uvicorn.run(app, host="0.0.0.0", port=port)