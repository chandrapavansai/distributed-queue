import random

import requests
from api import crud
from database import db

REPLICA_COUNT = 3

# utility functions for using the hash ring


def get_active_brokers(cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT DISTINCT broker_id FROM Broker")
    id_list = cursor.fetchall()
    if id_list is None:
        return 0
    return [broker_id[0] for broker_id in id_list]


def add_broker(url: str, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    # Check if broker already exists
    cursor.execute("SELECT broker_id FROM Broker WHERE url = %s", (url,))
    broker_id = cursor.fetchone()
    if broker_id is not None:
        return broker_id[0]
    cursor.execute("SELECT MAX(broker_id) FROM Broker")
    id = cursor.fetchone()[0]
    if id is None:
        id = 0
    else:
        id += 1
    cursor.execute(
        "INSERT INTO Broker (broker_id, url) VALUES (%s, %s)", (id, url,))
    return id


def remove_brokers(remove_ids_list: list, cursor=None):
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
            "SELECT topic_name, partition_id FROM Topic_Broker WHERE broker_id = %s", (id,))
        for topic, partition in cursor.fetchall():
            transfer_list.append((topic, partition,))

    # Do not delete partition details, as it is used in other tables
    # Redistribute partitions only if there are active brokers remaining
    if no_of_brokers > 0:
        for topic, partition in transfer_list:
            new_broker_id = active_brokers[random.randint(
                0, no_of_brokers - 1)]
            crud.update_partition_broker(
                new_broker_id, topic, partition, cursor)
    else:
        for topic, partition in transfer_list:
            crud.update_partition_broker(None, topic, partition, cursor)

    for id in remove_ids_list:
        cursor.execute("DELETE FROM Broker WHERE broker_id = %s", (id,))

    return no_of_brokers


def assign_brokers_to_new_partition(topic: str, partition: int, cursor=None):
    """
    Endpoint to create a partition for a topic
    :param topic: name of the topic
    :param partition: partition number
    :param cursor: database cursor
    :return: assigned broker id
    """
    active_brokers = get_active_brokers()
    if cursor is None:
        cursor = db.cursor()

    no_of_brokers = len(active_brokers)

    if no_of_brokers < REPLICA_COUNT:
        raise Exception("Not enough brokers to create partition")

    # Select 3 brokers at random from all the active brokers
    broker_ids = random.sample(active_brokers, REPLICA_COUNT)

    # We get the ids of the brokers
    broker_ips = [crud.get_broker_url(broker_id, cursor)
                  for broker_id in broker_ids]

    broker_raft_ips = []

    # Get the free port for each broker
    for broker_ip in broker_ips:
        try:
            freeport = requests.get(f"{broker_ip}/freeport").json()
        except:
            raise Exception("Could not connect to broker")

        if freeport == -1:
            raise Exception("Could not find free port for broker")

        if broker_ip.startswith("http://") or broker_ip.startswith("https://"):
            broker_ip = broker_ip.split("//")[1]

        broker_host = broker_ip.split(":")[0]
        broker_raft_ips.append(f"{broker_host}:{freeport}")

    for broker_ip in broker_ips:
        # Send a request to the broker to create a new partition
        try:
            requests.post(f"{broker_ip}/new", json={
                "topic": topic, "partition": partition, "partners": broker_raft_ips})
        except:
            raise Exception("Could not connect to broker")

    for new_broker_id in broker_ids:
        crud.set_partition_broker(new_broker_id, topic, partition, cursor)

    return broker_ids


def assign_broker_to_old_partition(topic: str, partition: int, cursor=None):
    """
    Endpoint to create a partition for a topic
    :param topic: name of the topic
    :param partition: partition number
    :param cursor: database cursor
    :return: assigned broker id
    """
    active_brokers = get_active_brokers()
    if cursor is None:
        cursor = db.cursor()

    no_of_brokers = len(active_brokers)

    if no_of_brokers > 0:
        new_broker_id = active_brokers[random.randint(0, no_of_brokers - 1)]
        crud.update_partition_broker(new_broker_id, topic, partition, cursor)
        return 0

    crud.update_partition_broker(None, topic, partition, cursor)
    return -1
