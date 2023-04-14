from database import db
from fastapi import APIRouter, HTTPException, status
import requests
import hashing
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


@router.post("/", status_code=status.HTTP_201_CREATED)
def create_topic(name: str, partitions: int = 1):
    """
    Endpoint to create a topic
    :param name: name of the topic
    :param partitions: number of partitions
    :return: success message
    """

    # Need to be a leader broker manager
    # Use consistent hashing and get the broker for the topic - first default partition
    cursor = db.cursor()

    if crud.topic_exists(name, cursor):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Topic with that name already exists")

    for i in range(partitions):
        hashing.assign_broker_to_new_partition(name, i, cursor)

    db.commit()
    return {"message": "Topic created"}


@router.post("/partitions", status_code=status.HTTP_201_CREATED)
def create_partition(topic: str):
    """
    Endpoint to create a partition for a topic
    :param topic: name of the topic
    :return: success message
    """

    # Need to be a leader broker manager
    cursor = db.cursor()

    if not crud.topic_exists(topic, cursor):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Topic with that name does not exist")

    new_partition = crud.get_partition_count(topic, cursor)

    selected_brokers = hashing.assign_broker_to_new_partition(topic, new_partition, cursor)
    ips = [crud.get_broker_url(broker, cursor) for broker in selected_brokers]

    # ? Change this later
    # Change port in ips to 9000
    ips = [ip.replace(":8000", ":9000") for ip in ips]

    # Send a message to each broker letting them know that they have a new partition
    for ip in ips:
        # Send the message
        requests.post(f"http://{ip}/new", json={"topic": topic, "partition": new_partition, "partners": ips})

    db.commit()
    return {"message": "Partition created", "partition": new_partition}
