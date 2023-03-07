import os
from datetime import datetime, timedelta
from time import sleep
from threading import Thread
import requests
import hashing as hashing

from api import crud
from database import db

url = os.environ.get("MGR_URL")
is_leader = os.environ.get("MGR_LEADER_URL") == url

ACTIVITY_TIMEOUT = 10 # seconds

class HeartbeatThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        while True:
            sleep(2)
            print("Running heartbeat algorithm")
            heartbeat_algorithm()


def claim_existence():
    """
    Utility function to claim existence of the manager
    """
    cursor = db.cursor()
    print("Creating manager", url, is_leader)
    crud.create_manager(url, is_leader, cursor)
    db.commit()


def heartbeat_algorithm():
    """
    Utility function to run the heartbeat algorithm
    """

    cursor = db.cursor()

    # Get the list of managers
    managers = crud.get_alive_managers(cursor)
    for manager_url, _ in managers:
        try:
            requests.get(manager_url + "/ping")
        except requests.exceptions.ConnectionError:
            crud.delete_manager(manager_url, cursor)
            db.commit()
            print("Deleted manager", manager_url)

    # Get the list of brokers
    brokers = crud.get_broker_ids(cursor)
    for broker_id in brokers:
        broker_url = crud.get_broker_url(broker_id, cursor)
        try:
            requests.get(broker_url + "/ping")
        except requests.exceptions.ConnectionError:
            hashing.remove_brokers([broker_id,], cursor)
            db.commit()
            print("Deleted broker", broker_id)

    # Get the list of consumers
    consumers = crud.get_consumers(cursor)
    for consumer_id in consumers:
        # Check if the consumer is alive
        if datetime.now() - crud.get_consumer_heartbeat(consumer_id, cursor) > timedelta(seconds=ACTIVITY_TIMEOUT):
            # If not alive, delete the consumer
            crud.delete_consumer(consumer_id, cursor)
            db.commit()
            print("Deleted consumer", consumer_id)

    # Get the list of producers
    producers = crud.get_producers(cursor)
    for producer_id in producers:
        # Check if the producer is alive
        if datetime.now() - crud.get_producer_heartbeat(producer_id, cursor) > timedelta(seconds=ACTIVITY_TIMEOUT):
            # If not alive, delete the producer
            crud.delete_producer(producer_id, cursor)
            db.commit()
            print("Deleted producer", producer_id)
