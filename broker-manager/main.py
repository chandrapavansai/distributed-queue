from fastapi import FastAPI, HTTPException
# from database import db
import requests

app = FastAPI()


# TODO:
# Run the Leader Selection Algorithm
# Run the Heartbeat Algorithm
# Run the Data Synchronization Algorithm

"""
Manage the data:
1. Clients:
    - Current Parition w.r.t to the topic - For Round Robin
    - Last fetched offset
    - Alive or not

2. Brokers:
    - List of live brokers
    - Hash table of topic parition to broker mapping
"""

"""
API Calls:
1. Leader
    - New Broker
    - New Partition
    - Creation of topic
    - Register of Producer, Consumer to a topic - /producer/register, /consumer/register
    - Consumer Dequeue - Get the message from the queue (Offset change) - /consumer/consume

2. Follower - Any API call which does not change the metadata of the system
    - Producer Enqueue - Add the message to the queue - /producer/produce
    - Get size of the queue - /size
    - Heartbeat - /heartbeat
    - Data Synchronization - /sync
"""

# Search for WAL_TAG in the code to find the places where the WAL is to be used

# Middleware to forward the request to the leader

# Function to validate the requests


def validate_request():
    pass


brokers = [
    {
        "IP_addr": "http://broker1:8000",
        "is_alive": True
    },
    {
        "IP_addr": "http://broker2:8000",
        "is_alive": True
    },
    {
        "IP_addr": "http://broker3:8000",
        "is_alive": True
    },
]

map_topic_parition_to_broker = {
    "topic1": {
        1: brokers[0],
        2: brokers[1]
    },
    "topic2": {
        1: brokers[0],
        2: brokers[1],
        3: brokers[2]
    }
}


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.get("/topics")
@app.get("/topics/paritions")
def list_topics(post : str = None):
    """
    Endpoint to list all topics and paritions
    :return: list of topics and paritions
    """
    return {"topics": {
        "topic1": [1, 2],
        "topic2": [1, 2, 3]
    }}
    pass


@app.post("/topics")
def create_topic(name: str):
    """
    Endpoint to create a topic
    :param name: name of the topic
    :return: success message
    """
    pass


@app.post("/topics/paritions")
def create_parition(topic: str, parition: int):
    """
    Endpoint to create a parition for a topic
    :param topic: name of the topic
    :param parition: parition number
    :return: success message
    """

    # WAL_TAG

    pass


@app.post("/consumer/register")
def register_consumer(topic: str):
    """
    Endpoint to register a consumer for a topic
    :param topic: the topic to which the consumer wants to subscribe
    :return: consumer id
    """
    # Insert the entry in the database
    # Return the consumer id

    # WAL_TAG

    pass


@app.post("/producer/register")
def register_producer(topic: str, parition: int = None):
    """
    Endpoint to register a producer for a topic
    :param topic: the topic to which the producer wants to publish
    :return: producer id
    """

    # WAL_TAG

    pass


@app.get("/consumer/consume")
def dequeue(topic: str, consumer_id: str, parition: int = None):
    """
    Endpoint to dequeue a message from the queue
    :param topic: the topic from which the consumer wants to dequeue
    :param consumer_id: consumer id obtained while registering
    :return: log message
    """

    # Need to switch the map_topic_parition_to_broker to a database

    if topic not in map_topic_parition_to_broker:
        raise HTTPException(status_code=404, detail="Topic does not exist")

    # Get the broker for the topic and parition
    if parition is None:
        # Get the parition number from the database and do Round Robin
        pass

    if parition not in map_topic_parition_to_broker[topic]:
        raise HTTPException(status_code=404, detail="Parition does not exist")

    IP_addr = map_topic_parition_to_broker["topic1"][parition]["IP_addr"]

    # Get the message from the broker
    response = requests.get(f"{IP_addr}/consumer/consume", params={
                            "topic": topic, "consumer_id": consumer_id, "parition": parition})
    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())

    # WAL_TAG

    pass


@app.post("/producer/produce")
def enqueue(topic: str, producer_id: str, message: str, parition: int = None):
    """
    Endpoint to enqueue a message to the queue
    :param topic: the topic to which the producer wants to enqueue
    :param producer_id: producer id obtained while registering
    :param message: log message to be enqueued
    """

    # NOTE:
    # Has to be a leader broker manager

    # Need to switch the map_topic_parition_to_broker to a database

    if topic not in map_topic_parition_to_broker:
        raise HTTPException(status_code=404, detail="Topic does not exist")

    if parition is None:
        # Get the parition number from the database and do Round Robin
        pass

    if parition not in map_topic_parition_to_broker[topic]:
        raise HTTPException(status_code=404, detail="Parition does not exist")

    IP_addr = map_topic_parition_to_broker["topic1"][parition]["IP_addr"]

    # Send the message to the broker
    response = requests.post(f"{IP_addr}/producer/produce", params={"topic": topic,
                             "producer_id": producer_id, "message": message, "parition": parition})
    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())

    pass


@app.get("/size")
async def size(topic: str, consumer_id: str, parition: int = None):
    """
    Endpoint to get the size of the queue for a given topic
    :param topic: the topic for which the size is to be obtained
    :param consumer_id: consumer id obtained while registering
    :param parition: parition number
    :return: size of the queue
    """
    pass


@app.get("/heartbeat")
def heartbeat():
    """
    Endpoint to send heartbeat
    :return: success message
    """
    return {"message": "alive"}
    pass


@app.get("/sync")
def sync():
    """
    Endpoint to sync data
    :return: success message
    """
    pass
