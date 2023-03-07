from uuid import uuid4

from . import crud
import requests
from fastapi import APIRouter, HTTPException

from database import db

router = APIRouter(
    prefix="/producer",
)

# Path: broker-manager\api\producer.py


@router.get("/produce")
async def enqueue(topic: str, producer_id: str, message: str, partition: int = None):
    """
    Endpoint to enqueue a message to the queue
    :param topic: the topic to which the producer wants to enqueue
    :param producer_id: producer id obtained while registering
    :param message: log message to be enqueued
    """

    # NOTE:
    # Has to be a leader broker manager

    cursor = db.cursor()

    if not crud.producer_exists(producer_id, cursor):
        raise HTTPException(status_code=404, detail="Producer does not exist")

    if not crud.topic_registered_producer(producer_id, topic, cursor):
        raise HTTPException(
            status_code=403, detail="Producer is not registered to this topic")

    if partition is None:
        # Get the partition number from the database and do Round Robin, and set the next partition
        partition = crud.get_round_robin_partition_producer(
            producer_id, topic, cursor)

    if not crud.partition_exists(topic, partition, cursor):
        raise HTTPException(status_code=404, detail="Partition does not exist")

    # Get the broker for the topic and partition
    broker_num = crud.get_related_broker(topic, partition, cursor)
    IP_addr = crud.get_broker_ip(broker_num, cursor)

    # Send the message to the broker
    response = requests.post(f"{IP_addr}/messages", json={
        "topic": topic,
        "content": message,
        "partition": partition})

    if response.ok:
        crud.increment_size(topic, partition, cursor)
        db.commit()  # Update the round robin partition
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())


@router.post("/register")
async def register_producer(topic: str, partition: int = None):
    """
    Endpoint to register a producer for a topic
    :param topic: the topic to which the producer wants to publish
    :return: producer id
    """

    # Insert the entry in the database
    # Return the producer id

    producer_id = str(uuid4())

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=404, detail="Topic does not exist")

    is_round_robin = partition is None
    if partition is None:
        partition = 0

    if not crud.partition_exists(topic, partition, cursor):
        raise HTTPException(
            status_code=404, detail="Partition does not exist")

    crud.register_producer(producer_id, topic, partition,
                           is_round_robin, cursor)

    db.commit()  # Update the producer table entries
    return producer_id
