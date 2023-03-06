from uuid import uuid4

import crud
import requests
from fastapi import APIRouter, HTTPException

from ..metadata import Client

router = APIRouter(
    prefix="/consumer",
)

# Path: broker-manager\api\consumer.py


@router.get("/consume")
async def consume(topic: str, consumer_id: str, parition: int = None):
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

    if not crud.topic_registered(consumer_id, topic, cursor):
        raise HTTPException(
            status_code=403, detail="Consumer is not registered to this topic")

    if parition is None:
        # Get the parition number from the database and do Round Robin, and set the next parition
        parition = crud.get_round_robin_parition(consumer_id, topic, cursor)

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

    db.commit()  # Update the round robin parition

    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(status_code=response.status_code,
                            detail=response.json())

    # WAL_TAG


@router.post("/register")
def register_consumer(topic: str, parition: int = None):
    """
    Endpoint to register a consumer for a topic
    :param topic: the topic to which the consumer wants to subscribe
    :return: consumer id
    """
    # Insert the entry in the database
    # Return the consumer id

    cursor = db.cursor()

    consumer_id = uuid4()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(status_code=404, detail="Topic does not exist")

    if not crud.parition_exists(topic, parition, cursor):
        raise HTTPException(status_code=404, detail="Parition does not exist")

    crud.register_consumer(consumer_id, topic, parition, cursor)

    db.commit()  # Update the consumer table entries
    return consumer_id

    # WAL_TAG
