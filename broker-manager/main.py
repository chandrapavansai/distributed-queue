from fastapi import FastAPI, HTTPException
from uuid import uuid4
from hash_ring import HashRing
import heapq
import crud

from .api import consumer, producer, topics, heartbeat, managers

app = FastAPI()
ring = HashRing([])

app.include_router(consumer.router)
app.include_router(producer.router)
app.include_router(topics.router)
app.include_router(managers.router)
app.include_router(heartbeat.router)


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

def setup_hashring(brokers: list):
    """
    Utility function to setup the consistent hash function
    :param brokers : list of the ip addresses of the brokers
    """
    ring = HashRing(brokers)


def add_broker(ip: str):
    """
    Utility function to add the new broker to the hash ring
    :param ip : ip address of the new broker 
    """
    ring.add_node(ip, {'weight': 1})


def remove_broker(ip: str):
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
for i in range(0, len(brokers_table)):
    broker_ip_to_id[brokers_table[i]["IP_addr"]] = i


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

# datastructures and utility functions for implementing round robin broker selection
cur_broker_id = {}
for topic in topic_parition_to_broker_table:
    cur_broker_id[topic] = 0


# TODO: Leader election algorithm
leader_manager = "http://manager2:5000"


# TODO: Consistent hashing algorithm
# Assign broker to new parition
def assign_broker_to_new_parition(topic: str, partition: int):
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


@app.get("/sync")
def sync():
    """
    Endpoint to sync data
    :return: success message
    """
    pass
