from fastapi import FastAPI, HTTPException
from database import db
from uuid import uuid4
from hash_ring import HashRing
import heapq
import requests
import crud


app = FastAPI()
ring = HashRing([])






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




# utility functions for using the hash ring

def setup_hashring(brokers : list):
    """
    Utility function to setup the consistent hash function
    :param brokers : list of the ip addresses of the brokers
    """
    ring = HashRing(brokers)


def add_broker(ip : str):
    """
    Utility function to add the new broker to the hash ring
    :param ip : ip address of the new broker 
    """
    ring.add_node(ip, {'weight': 1})


def remove_broker(ip : str):
    """
    Utility function to remove an existing broker from the hash ring
    :param ip : ip address of the broker to be removed 
    """
    ring.remove_node(ip)




def validate_request():
    pass


brokers_table = [
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

broker_ip_to_id = []
for i in range(0,len(brokers_table)):
    broker_ip_to_id[brokers_table[i]["IP_addr"]] = i





managers_table = [
    {
        "IP_addr": "http://manager1:5000",
        "is_alive": False
    },
    {
        "IP_addr": "http://manager2:5000",
        "is_alive": True
    },
    {
        "IP_addr": "http://manager3:5000",
        "is_alive": True
    },
]

topic_parition_to_broker_table = {
    "topic1": {
        1: 0,   # Broker 0
        2: 1    # Broker 1
    },
    "topic2": {
        1: 0,   # Broker 0
        2: 1,   # Broker 1
        3: 2    # Broker 2
    }
}

consumer_topic_table = {
    "consumer1": {
        "topic1": {
            "parition": 1,
            "round_robin": False,
        },
    },
    "consumer2": {
        "topic1": {
            "parition": 2,
            "round_robin": False,
        },
        "topic2": {
            "parition": 1,
            "round_robin": True,
        },
    }
}

producer_topic_table = {
    "producer1": {
        "topic1": {
            "parition": 1,
            "round_robin": False,
        },
    },
    "producer2": {
        "topic1": {
            "parition": 2,
            "round_robin": False,
        },
        "topic2": {
            "parition": 1,
            "round_robin": True,
        },
    }
}


# datastructures and utility functions for implementing round robin broker selection
cur_broker_id = {}
for topic in topic_parition_to_broker_table:
    cur_broker_id[topic] = 0


# TODO: Leader election algorithm
leader_manager = "http://manager2:5000"


# TODO: Consistent hashing algorithm
# Assign broker to new parition
def assign_broker_to_new_parition(topic : str, partition : int):
    """
    Endpoint to create a parition for a topic
    :param topic: name of the topic
    :param parition: parition number
    :return: assigned broker id
    """
    key = topic + "###" + str(partition)
    broker_ip = ring.get_node(key)
    return broker_ip_to_id[broker_ip]
    

# TODO: Hashing algorithm



@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.get("/managers")
def get_managers():

    cursor = db.cursor()
    return {
        "managers": [
            (manager_ip, manager_ip == leader_manager)
            for manager_ip in crud.get_alive_managers(cursor)
        ]
    }

    # No need to commit as we are not changing anything
    # db.commit()


@ app.get("/topics")
@ app.get("/topics/paritions")
def list_topics():
    """
    Endpoint to list all topics and paritions
    :return: list of topics and paritions
    """

    cursor = db.cursor()
    # Get the topics and paritions using dictionary comprehension
    return {
        "topics": [
            {
                topic: crud.get_paritions(topic, cursor)
            } for topic in crud.get_topics(cursor)
        ]
    }

    # No need to commit as we are not changing anything
    # db.commit()


@ app.post("/topics")
def create_topic(name: str):
    """
    Endpoint to create a topic
    :param name: name of the topic
    :return: success message
    """

    # Need to be a leader broker manager
    # Use consistent hashing and get the broker for the topic - first default parition
    cursor = db.cursor()

    if crud.topic_exists(name, cursor):
        raise HTTPException(
            status_code=400, detail="Topic with that name already exists")

    broker_num = assign_broker_to_new_parition(name, 0)
    topic_parition_to_broker_table[name] = {
        1: brokers_table[broker_num]
    }
    crud.create_topic(name, cursor)

    broker_num = assign_broker_to_new_parition()
    # By default, create a parition with ID 1
    crud.insert_parition_broker(name, 1, broker_num, cursor)

    db.commit()

    # WAL_TAG

    return {"message": "Topic created successfully"}


# TODO: Should we allow the partition parameter?
@app.post("/topics/paritions")
def create_parition(topic: str, partition: int):
    """
    Endpoint to create a parition for a topic
    :param topic: name of the topic
    :param parition: parition number
    :return: success message
    """

    # Need to be a leader broker manager

    # Use consistent hashing and get the broker for the parition
    broker_num = assign_broker_to_new_parition(topic, partition)

    if topic in topic_parition_to_broker_table and partition in topic_parition_to_broker_table[topic]:
        raise HTTPException(
            status_code=400, detail="Parition with that ID already exists")

    topic_parition_to_broker_table[topic] = {
        partition: brokers_table[broker_num]
    }
    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(
            status_code=400, detail="Topic with that name does not exist")

    if crud.parition_exists(topic, parition, cursor):
        raise HTTPException(
            status_code=400, detail="Parition with that ID already exists")

    broker_num = assign_broker_to_new_parition()
    crud.insert_parition_broker(topic, parition, broker_num, cursor)

    db.commit()

    # WAL_TAG

    return {"message": "Parition created successfully"}


@ app.post("/consumer/register")
def register_consumer(topic: str, parition: int = None):
    """
    Endpoint to register a consumer for a topic
    :param topic: the topic to which the consumer wants to subscribe
    :return: consumer id
    """
    # Insert the entry in the database
    # Return the consumer id

    consumer_id = uuid4()
    consumer_topic_table[consumer_id] = {
        topic: {
            "parition": parition,
            "round_robin": (parition is None)
        }
    }

    return consumer_id
    # WAL_TAG

    pass


@app.post("/producer/register")
def register_producer(topic: str, parition: int = None):
    """
    Endpoint to register a producer for a topic
    :param topic: the topic to which the producer wants to publish
    :return: producer id
    """

    # Insert the entry in the database
    # Return the producer id

    producer_id = uuid4()
    producer_topic_table[producer_id] = {
        topic: {
            "parition": parition,
            "round_robin": (parition is None)
        }
    }

    return producer_id
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

    # NOTE:
    # Read-only broker managers

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=404, detail="Topic does not exist")

    if parition is None:
        # Get the parition number from the database and do Round Robin, and set the next parition
        parition = get_round_robin_parition(consumer_id, topic)

    if not crud.parition_exists(topic, parition, cursor):
        raise HTTPException(status_code=404, detail="Parition does not exist")

    # Get the broker for the topic and parition
    broker_num = crud.get_related_broker(topic, parition, cursor)
    IP_addr = crud.get_broker_ip(broker_num, cursor)

    # Get the message from the broker
    response = requests.get(f"{IP_addr}/consumer/consume", params={
                            "topic": topic,
                            "consumer_id": consumer_id,
                            "parition": parition})

    db.commit()

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())

    # No need to update the offset in the database as the offset is updated in the broker
    # So, read-nothing property is maintained


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

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=404, detail="Topic does not exist")

    if parition is None:
        # Get the parition number from the database and do Round Robin, and set the next parition
        parition = get_round_robin_parition(producer_id, topic)

    if not crud.parition_exists(topic, parition, cursor):
        raise HTTPException(status_code=404, detail="Parition does not exist")

    # Get the broker for the topic and parition
    broker_num = crud.get_related_broker(topic, parition, cursor)
    IP_addr = crud.get_broker_ip(broker_num, cursor)

    # Send the message to the broker
    response = requests.post(f"{IP_addr}/producer/produce", params={
        "topic": topic,
        "producer_id": producer_id,
        "message": message,
        "parition": parition})

    db.commit()

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())

    # WAL_TAG : No need to update actually as the offset is updated in the broker


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
