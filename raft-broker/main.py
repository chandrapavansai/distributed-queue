import time
import os
import uvicorn
import requests
import argparse
from fastapi import FastAPI, status, Depends, HTTPException, Query, Response
import schemas
from broker import Broker

app = FastAPI()

leader_url = os.getenv("LEADER_URL")  # "http://localhost:8000"
broker = None
broker_host = os.getenv("BROKER")  # "localhost"
pause = False

@app.on_event("startup")
async def ping_manager():
    print(leader_url)
    print(broker_host)
    print("leader_url", leader_url, "broker_host",
          broker_host)   # Create Broker object

    # ! For testing purposes
    # config = {
    #     'test': {
    #         1: ['raft-broker1:9000','raft-broker2:9000','raft-broker3:9000'],
    #     }
    # }

    global broker
    broker = Broker({}, broker_host)

    broker_url = 'http://' + broker_host + ':' + '8000'
    try:
        requests.post(f"{leader_url}/broker?url={broker_url}")
        print("Running in manager-connected mode ...")
    except:
        print("Running in detached mode ...")
    return


@app.middleware("http")
async def add_process_time_header(request, call_next):
    # Check if its get request for /pause
    if not (request.method == "GET" and request.url.path == "/pause"):
        global pause
        if pause:
            return Response(status_code=503)
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
    if message is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No message found")
    result = schemas.Message(id=offset, content=message)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No message found")
    return result


@app.post("/messages", status_code=status.HTTP_201_CREATED)
def post_message(message: schemas.MessageCreate):
    # partners = partners[0].split(',')
    print(message)
    len = broker.create_message(
        message.topic, message.partition, message.content)
    if len is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="No message found")
    return {"message": "Message added successfully", "queue_len": len}


@app.get("/messages/count")
def get_message_count(topic: str, partition: int, offset: int = 0):
    cnt = broker.get_message_count(topic, partition, offset)
    # If cnt is None, raise http Exception
    if cnt is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Topic '{topic}' - partition '{partition}' not found")
    return cnt

# delete a topic-partition

@app.get("/messages/delete")
def delete_message(topic: str, partition: int):
    return broker.delete_topic(topic, partition)


@app.post("/new")
# def add_new(topic: str, partition: int, partners: list = Query([], alias="partners")):
def add_new(info: schemas.TopicCreate):
    # partners = partners[0].split(',')
    # Creating new topic-partition
    print(info.topic)
    print(info.partition)
    print(info.partners)
    return broker.create_topic(info.topic, info.partition, info.partners)


@app.get("/freeport")
def get_free_port():
    return broker.get_free_port()

@app.get("/pause")
def pause_broker():
    global pause
    if not pause:
        pause = True
        return {"message": "Broker paused"}
    else:
        pause = False
        return {"message": "Broker unpaused"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the FastAPI server.")
    parser.add_argument("--port", type=int, default=8000,
                        help="Port number to start the server on.")
    args = parser.parse_args()
    port = args.port
    uvicorn.run(app, host="0.0.0.0", port=port)
