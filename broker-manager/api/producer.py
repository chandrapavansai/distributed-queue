from uuid import uuid4

import crud
import requests
from fastapi import APIRouter, HTTPException

from ..metadata import Client

router = APIRouter(
    prefix="/producer",
)

# Path: broker-manager\api\producer.py


@router.get("/produce")
async def enqueue(topic: str, producer_id: str, message: str, parition: int = None):
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

    if not crud.topic_registered(producer_id, topic, cursor):
        raise HTTPException(
            status_code=403, detail="Producer is not registered to this topic")

    if parition is None:
        # Get the parition number from the database and do Round Robin, and set the next parition
        parition = crud.get_round_robin_parition(producer_id, topic, cursor)

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

    db.commit() # Update the round robin parition

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())


@router.post("/register")
async def register_producer(topic: str, parition: int = None):
    """
    Endpoint to register a producer for a topic
    :param topic: the topic to which the producer wants to publish
    :return: producer id
    """

    # Insert the entry in the database
    # Return the producer id

    producer_id = uuid4()

    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=404, detail="Topic does not exist")

    if parition is not None:
        if not crud.parition_exists(topic, parition, cursor):
            raise HTTPException(
                status_code=404, detail="Parition does not exist")

    crud.register_producer(producer_id, topic, parition, cursor)

    db.commit() # Update the producer table entries
    return producer_id
