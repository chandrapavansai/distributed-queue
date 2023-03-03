import os
import redis
import json

# Abstract database class
class Database:
    # Static variable
    db = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=os.getenv("REDIS_PORT", 6379), db=0)

    def __init__(self):
        pass

    @staticmethod
    def populate():
        # Populate database with sample data
        # Add client data
        clients = {
            "1": {
                "live": 1,
                "topic_pos": {
                    "topic1": 0,
                    "topic2": 0
                },
                "topic_partition": {
                    "topic1": [0, 1, 2],
                    "topic2": [0, 1, 2]
                }
            },
            "2": {
                "live": 1,
                "topic_pos": {
                    "topic1": 1,
                    "topic2": 1
                },
                "topic_partition": {
                    "topic1": [0, 1, 2],
                    "topic2": [0, 1, 2]
                },
            },
        }

        brokers = {
            "1": {
                "live": 1,
                "topic_partition": {
                    "topic1": [0],
                    "topic2": [1]
                }
            },
            "2": {
                "live": 1,
                "topic_partition": {
                    "topic1": [1],
                    "topic2": [2]
                }
            },
            "3": {
                "live": 1,
                "topic_partition": {
                    "topic1": [2],
                    "topic2": [0]
                }
            },
        }

        managers = {
            "1": {
                "live": 1,
            },
            "2": {
                "live": 1,
            },
            "3": {
                "live": 1,
            },
        }

        for client_id, client_data in clients.items():
            Database.db.hset('client:'+client_id,json.dumps(client_data))
        
        for broker_id, broker_data in brokers.items():
            Database.db.hset('broker:'+broker_id,json.dumps(broker_data))
        
        for manager_id, manager_data in managers.items():
            Database.db.hset('manager:'+manager_id,json.dumps(manager_data))