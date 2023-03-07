import random

from api import crud
from database import db


# utility functions for using the hash ring
def get_active_brokers():
    """
    Utility function to return the id's of active brokers
    returns a list of active broker id's
    """
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT broker_id FROM Broker")
    return [id[0] for id in cursor.fetchall()]


def add_broker(ip: str, cursor):
    """
    Utility function to add the new broker to the hash ring
    :param ip : ip address of the new broker 
    """
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT MAX(broker_id) FROM Broker")
    id = cursor.fetchone()[0]
    if id is None:
        id = 0
    else:
        id += 1
    cursor.execute(
        "INSERT INTO Broker (broker_id, ip_addr) VALUES (%s, %s)", (id, ip,))
    return id


def remove_brokers(remove_ids_list: list, cursor):
    """
    Utility function to remove an existing broker from the hash ring
    :param ip : ip address of the broker to be removed 
    """
    if cursor is None:
        cursor = db.cursor()
    active_brokers = get_active_brokers()
    no_of_brokers = len(active_brokers)

    for id in remove_ids_list:
        if id not in active_brokers:
            return -1
        else:
            active_brokers.remove(id)

    no_of_brokers -= len(remove_ids_list)

    transfer_list = []
    for id in remove_ids_list:
        cursor.execute(
            "SELECT topic_name, partition_id FROM Topic WHERE broker_id = %s", (id,))
        for topic, partition in cursor.fetchall():
            transfer_list.append((topic, partition,))

    # Do not delete partition details, as it is used in other tables
    for topic, partition in transfer_list:
        new_broker_id = active_brokers[random.randint(0, no_of_brokers-1)]
        crud.update_partition_broker(new_broker_id, topic, partition, cursor)

    for id in remove_ids_list:
        cursor.execute("DELETE FROM Broker WHERE broker_id = %s", (id,))

    return no_of_brokers


def assign_broker_to_new_partition(topic: str, partition: int, cursor):
    """
    Endpoint to create a partition for a topic
    :param topic: name of the topic
    :param partition: partition number
    :return: assigned broker id
    """
    active_brokers = get_active_brokers()
    if cursor is None:
        cursor = db.cursor()

    no_of_brokers = len(active_brokers)
    new_broker_id = active_brokers[random.randint(0, no_of_brokers - 1)]
    crud.set_partition_broker(new_broker_id, topic, partition, cursor)
