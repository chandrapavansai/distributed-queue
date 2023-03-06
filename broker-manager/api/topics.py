from database import db
from fastapi import APIRouter, HTTPException

from . import crud

router = APIRouter(
    prefix="/topics",
)


@router.get("/")
@router.get("/paritions")
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


@router.post("/")
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

    # broker_num = assign_broker_to_new_parition(name, 0)
    # topic_parition_to_broker_table[name] = {
    #     1: brokers_table[broker_num]
    # }
    # crud.create_topic(name, cursor)

    # broker_num = assign_broker_to_new_parition()
    broker_num = 0
    # By default, create a parition with ID 1
    crud.set_partition_broker(broker_num, name, 0, cursor)

    db.commit()

    # WAL_TAG

    return {"message": "Topic created successfully"}


# TODO: Should we allow the partition parameter?
router.post("/paritions")


# def create_parition(topic: str, partition: int):
#     """
#     Endpoint to create a parition for a topic
#     :param topic: name of the topic
#     :param parition: parition number
#     :return: success message
#     """

#     # Need to be a leader broker manager

#     # Use consistent hashing and get the broker for the parition
#     broker_num = assign_broker_to_new_parition(topic, partition)

#     if topic in topic_parition_to_broker_table and partition in topic_parition_to_broker_table[topic]:
#         raise HTTPException(
#             status_code=400, detail="Parition with that ID already exists")

#     topic_parition_to_broker_table[topic] = {
#         partition: brokers_table[broker_num]
#     }
#     cursor = db.cursor()

#     if not crud.topic_exists(topic, cursor):
#         raise HTTPException(
#             status_code=400, detail="Topic with that name does not exist")

#     if crud.parition_exists(topic, parition, cursor):
#         raise HTTPException(
#             status_code=400, detail="Parition with that ID already exists")

#     broker_num = assign_broker_to_new_parition()
#     crud.insert_parition_broker(topic, parition, broker_num, cursor)

#     db.commit()

#     # WAL_TAG

#     return {"message": "Parition created successfully"}
