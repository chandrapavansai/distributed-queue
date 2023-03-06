import database as db
import random
from ..api import crud


active_brokers = {}
no_of_brokers = 0

# utility functions for using the hash ring

def active():
    """
    Utility function to return the id's of active brokers
    returns a list of active broker id's
    """
    cursor = db.cursor()
    cursor.execute("SELECT DISTINCT broker_id FROM Broker")
    return cursor.fetchall()



def add_broker(ip: str):
    """
    Utility function to add the new broker to the hash ring
    :param ip : ip address of the new broker 
    """

    cursor = db.cursor()
    cursor.execute("SELECT MAX(broker_id) FROM Broker")
    id = cursor.fetchone()[0] + 1
    cursor.execute("INSERT INTO Broker (broker_id, ip_addr) VALUES (%s, %s)", (id, ip,))
    return id


def remove_brokers(list_ids: list):
    """
    Utility function to remove an existing broker from the hash ring
    :param ip : ip address of the broker to be removed 
    """


    active_brokers = active()
    cursor = db.cursor()
    no_of_brokers = len(active_brokers)
    for id in list_ids:
        if id not in active_brokers:
            return -1
        else:
            active_brokers.remove(id)
        
    no_of_brokers -= len(list_ids)

    transfer_list = []
    for id in list_ids:
        cursor.execute("SELECT topic_name, parition_num FROM Topic WHERE broker_id = %s", (id,))
        for topic, partition in cursor.fetchall():
            transfer_list.append(tuple(topic, partition))

    for id in list_ids:
        cursor.execute("DELETE FROM Topic WHERE broker_id = %s", (id,))

    for topic, partition in transfer_list:
        new_broker_id = active_brokers[random.randint(0,no_of_brokers-1)]
        crud.insert_parition_broker(topic, partition, new_broker_id)
    
    for id in list_ids:
        cursor.execute("DELETE FROM Broker WHERE broker_id = %s", (id,))
    return no_of_brokers

    


def assign_broker_to_new_parition(topic: str, partition: int):
    """
    Endpoint to create a parition for a topic
    :param topic: name of the topic
    :param parition: parition number
    :return: assigned broker id
    """
    active_brokers = active()
    cursor = db.cursor()
    no_of_brokers = len(active_brokers)
    new_broker_id = active_brokers[random.randint(0,no_of_brokers-1)]
    crud.insert_parition_broker(topic, partition, new_broker_id)



def get_round_robin_parition_consumer(consumer_id, topic):
    cursor = db.cursor()
    i = 0
    cursor.execute("SELECT COUNT(*) FROM Topic WHERE topic_id = %s", (topic,))
    size = cursor.fetchone()[0]
    cursor.execute("SELECT parition_id FROM Consumer WHERE consumer_id = %s", (consumer_id,))
    partition = cursor.fetchone()[0]
    while 1:
        cursor.execute("SELECT offset_val FROM ConsumerPartition WHERE consumer_id = %s AND parition_id = %s", (consumer_id, (partition+i)%size,))
        offset = cursor.fetchone()[0]
        max_offset = 0
        if offset != max_offset: 
            return (partition+i)%size
        
    return 0
