from uuid import uuid4

import hashing as hashing
import requests
from database import db
from fastapi import APIRouter, HTTPException, status
from topic_locks import get_lock

from . import crud

# Path: broker-manager\api\producer.py

router = APIRouter(prefix="/producer")


@router.post("/produce")
async def enqueue(topic: str, producer_id: str, message: str, partition: int = None):
    """
    Endpoint to enqueue a message to the queue
    :param topic: the topic to which the producer wants to enqueue
    :param producer_id: producer id obtained while registering
    :param message: log message to be enqueued
    :param partition: partition number to which the message is to be enqueued
    """

    # NOTE:
    # Has to be a leader broker manager

    async with get_lock(topic):
        cursor = db.cursor()

        if not crud.producer_exists(producer_id, cursor):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producer does not exist")

        if not crud.topic_registered_producer(producer_id, topic, cursor):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Producer is not registered to this topic")

        if partition is None:
            # Get the partition number from the database and do Round Robin, and set the next partition
            partition = crud.get_round_robin_partition_producer(
                producer_id, topic, cursor)
            
        if not crud.partition_registered_producer(producer_id, topic, partition, cursor):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Producer is not registered to this partition")

        if not crud.partition_exists(topic, partition, cursor):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Partition does not exist")

        # Get the broker for the topic and partition
        broker_num = crud.get_related_broker(topic, partition, cursor)

        # Greedy approach to handle broker failure
        if broker_num is None:
            if hashing.assign_broker_to_old_partition(topic, partition, cursor) == -1:
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                                    detail="Unable to process request, No brokers available")
            
        broker_url = crud.get_broker_url(broker_num, cursor)

        # Send the message to the broker
        try:
            response = requests.post(f"{broker_url}/messages", json={
                "topic": topic,
                "content": message,
                "partition": partition})
        except requests.exceptions.ConnectionError:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                                detail="Unable to process request, Broker is not available")

        if response.ok:
            crud.increment_size(topic, partition, cursor)
            db.commit()  # Update the round-robin partition
            return response.json()
        else:
            raise HTTPException(status_code=response.status_code,
                                detail=response.json()['detail'])


@router.post("/register")
async def register_producer(topic: str, partition: int = None):
    """
    Endpoint to register a producer for a topic
    :param topic: the topic to which the producer wants to publish
    :param partition: the partition to which the producer wants to publish
    :return: producer id
    """

    # Insert the entry in the database
    # Return the producer id

    producer_id = str(uuid4())

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Topic does not exist")

    is_round_robin = partition is None
    if partition is None:
        partition = 0

    if not crud.partition_exists(topic, partition, cursor):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Partition does not exist")

    crud.register_producer(producer_id, topic, partition,
                           is_round_robin, cursor)

    db.commit()  # Update the producer table entries
    return producer_id
