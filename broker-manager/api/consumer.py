from uuid import uuid4

import requests
from database import db
from fastapi import APIRouter, HTTPException

from . import crud

router = APIRouter(
    prefix="/consumer",
)

# Path: broker-manager\api\consumer.py


@router.get("/consume")
async def consume(topic: str, consumer_id: str, partition: int = None):
    """
    Endpoint to dequeue a message from the queue
    :param topic: the topic from which the consumer wants to dequeue
    :param consumer_id: consumer id obtained while registering
    :return: log message
    """

    # NOTE:
    # Read-only broker managers

    cursor = db.cursor()

    if not crud.consumer_exists(consumer_id, cursor):
        raise HTTPException(status_code=404, detail="Consumer does not exist")

    if not crud.topic_registered_consumer(consumer_id, topic, cursor):
        raise HTTPException(
            status_code=403, detail="Consumer is not registered to this topic")

    if partition is None:
        # Get the partition number from the database and do Round Robin, and set the next partition
        partition = crud.get_round_robin_partition_consumer(
            consumer_id, topic, cursor)

    if not crud.partition_exists(topic, partition, cursor):
        raise HTTPException(status_code=404, detail="Partition does not exist")

    offset = crud.get_offset(consumer_id, partition, cursor)

    # Get the broker for the topic and partition
    broker_num = crud.get_related_broker(topic, partition, cursor)
    IP_addr = crud.get_broker_ip(broker_num, cursor)

    # Get the message from the broker
    response = requests.get(f"{IP_addr}/messages", params={
                            "topic": topic,
                            "partition": partition,
                            "offset": offset})

    if response.ok:
        crud.increment_offset(consumer_id, partition, cursor)
        db.commit()  # Update the round robin partition
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())

    # WAL_TAG


@router.post("/register")
def register_consumer(topic: str, partition: int = None):
    """
    Endpoint to register a consumer for a topic
    :param topic: the topic to which the consumer wants to subscribe
    :return: consumer id
    """
    # Insert the entry in the database
    # Return the consumer id

    cursor = db.cursor()

    consumer_id = str(uuid4())

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=404, detail="Topic does not exist")

    is_round_robin = partition is None
    if partition is None:
        partition = 0

    if not crud.partition_exists(topic, partition, cursor):
        raise HTTPException(status_code=404, detail="Partition does not exist")

    crud.register_consumer(consumer_id, topic, partition,
                           is_round_robin, cursor)

    db.commit()  # Update the consumer table entries
    return consumer_id

    # WAL_TAG


@router.get("/size")
async def size(consumer_id: str, topic: str, partition: int = None):
    """
    Endpoint to get the size of the queue for a given topic
    :param topic: the topic for which the size is to be obtained
    :param consumer_id: consumer id obtained while registering
    :param partition: partition number
    :return: size of the queue
    """

    cursor = db.cursor()

    if not crud.consumer_exists(consumer_id, cursor):
        raise HTTPException(status_code=404, detail="Consumer does not exist")
    
    if not crud.topic_registered_consumer(consumer_id, topic, cursor):
        raise HTTPException(
            status_code=403, detail="Consumer is not registered to this topic")

    if partition is None:
        # Get messages to read from all partitions
        partitions = crud.get_partitions(topic, cursor)
        size = 0
        for partition in partitions:
            size += crud.get_size(topic, partition, cursor)
            size -= crud.get_offset(consumer_id, partition, cursor)
        return size
        
    if not crud.partition_exists(topic, partition, cursor):
        raise HTTPException(status_code=404, detail="Partition does not exist")

    size = crud.get_size(topic, partition, cursor)
    offset = crud.get_offset(consumer_id, partition, cursor)

    # No need to commit as we are not updating anything
    return size - offset
