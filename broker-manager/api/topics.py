from database import db
from fastapi import APIRouter, HTTPException

import hashing.hash as Hash
from . import crud

router = APIRouter(
    prefix="/topics",
)


@router.get("/")
@router.get("/partitions")
def list_topics():
    """
    Endpoint to list all topics and partitions
    :return: list of topics and partitions
    """

    cursor = db.cursor()
    # Get the topics and partitions using dictionary comprehension
    print(crud.get_topics(cursor))
    return {
        "topics": [
            {
                topic: crud.get_partitions(topic, cursor)
            } for topic in crud.get_topics(cursor)
        ]
    }

    # No need to commit as we are not changing anything
    # db.commit()


@router.post("/")
def create_topic(name: str, partition_count: int = 1):
    """
    Endpoint to create a topic
    :param name: name of the topic
    :return: success message
    """

    # Need to be a leader broker manager
    # Use consistent hashing and get the broker for the topic - first default partition
    cursor = db.cursor()

    if crud.topic_exists(name, cursor):
        raise HTTPException(
            status_code=400, detail="Topic with that name already exists")

    for i in range(partition_count):
        Hash.assign_broker_to_new_partition(name, i, cursor)

    db.commit()

    return {"message": "Topic created successfully"}


@router.post("/partitions")
def create_partition(topic: str):
    """
    Endpoint to create a partition for a topic
    :param topic: name of the topic
    :param partition: partition number
    :return: success message
    """

    # Need to be a leader broker manager
    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(
            status_code=400, detail="Topic with that name does not exist")
    
    new_partition = crud.get_partition_count(topic, cursor)

    if crud.partition_exists(topic, new_partition, cursor):
        raise HTTPException(
            status_code=400, detail="Partition with that ID already exists")
    
    Hash.assign_broker_to_new_partition(topic, new_partition, cursor)

    db.commit()

    return {"message": "Partition created successfully"}
