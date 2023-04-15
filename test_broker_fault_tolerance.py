import requests
import time

broker1_url = "http://localhost:8000"
broker2_url = "http://localhost:8001"
broker3_url = "http://localhost:8002"

def assertEqual(a, b):
    assert a == b

if __name__ == '__main__':
        # Create topic partition
        response = requests.post(f"{broker1_url}/new", json={
            "topic": "fault-tolerance",
            "partition": 1,
            "partners": ["raft-broker2:9000","raft-broker1:9000","raft-broker3:9000"]
        })
        assertEqual(response.status_code, 200)
        print("Created topic 'fault-tolerance' with partition '1' on broker 1")

        # Create topic partition
        response = requests.post(f"{broker2_url}/new", json={
            "topic": "fault-tolerance",
            "partition": 1,
            "partners": ["raft-broker2:9000","raft-broker1:9000","raft-broker3:9000"]
        })
        assertEqual(response.status_code, 200)
        print("Created topic 'fault-tolerance' with partition '1' on broker 2")


        # Create topic partition
        response = requests.post(f"{broker3_url}/new", json={
            "topic": "fault-tolerance",
            "partition": 1,
            "partners": ["raft-broker2:9000","raft-broker1:9000","raft-broker3:9000"]
        })
        assertEqual(response.status_code, 200)
        print("Created topic 'fault-tolerance' with partition '1' on broker 3")

        time.sleep(1)

        # Create a message on broker 1
        response = requests.post(f"{broker1_url}/messages", json={
            "topic": "fault-tolerance",
            "partition": 1,
            "content": "fault-message",
        })
        assertEqual(response.status_code, 201)
        print("Created message 'fault-message' on broker 1")

        print("Pausing broker 1")
        response = requests.get(f"{broker1_url}/pause")
        assertEqual(response.status_code, 200)

        # Test is broker 1 is paused - 503
        response = requests.get(f"{broker1_url}/ping")
        assertEqual(response.status_code, 503)
        print("Broker 1 is paused")


        # offset = response.json()["id"]
        # Check if the message exists on broker 2
        response = requests.get(f"{broker2_url}/messages",params={
            "topic": "fault-tolerance",
            "partition": 1,
            "offset": 0,
        })
        assertEqual(response.status_code, 200)
        assertEqual(response.json()["content"], "fault-message")
        print("Got message {} from broker 2".format(response.json()["content"]))

        # Check if the message exists on broker 3
        response = requests.get(f"{broker3_url}/messages",params={
            "topic": "fault-tolerance",
            "partition": 1,
            "offset": 0,
        })
        assertEqual(response.status_code, 200)
        assertEqual(response.json()["content"], "fault-message")
        print("Got message {} from broker 3".format(response.json()["content"]))

        # Add a new message to broker 2
        response = requests.post(f"{broker2_url}/messages", json={
            "topic": "fault-tolerance",
            "partition": 1,
            "content": "fault-message-2",
        })
        assertEqual(response.status_code, 201)
        print("Created message 'fault-message-2' on broker 2")


        print("Getting broker 1 back up")
        # Get broker1 back up
        response = requests.get(f"{broker1_url}/pause")
        assertEqual(response.status_code, 200)


        # Check if broker 1 is up
        response = requests.get(f"{broker1_url}/ping")
        assertEqual(response.status_code, 200)
        print("Broker 1 is back up")

        # Check if the message exists on broker 1
        response = requests.get(f"{broker1_url}/messages",params={
            "topic": "fault-tolerance",
            "partition": 1,
            "offset": 0,
        })
        assertEqual(response.status_code, 200)
        assertEqual(response.json()["content"], "fault-message")
        print("Got message {} from broker 1".format(response.json()["content"]))

        # Get length of queue from broker 1
        response = requests.get(f"{broker1_url}/messages/count",params={
            "topic": "fault-tolerance",
            "partition": 1,
        })
        assertEqual(response.status_code, 200)
        assertEqual(response.json(), 2)
        print("Got message count {} from broker 1".format(response.json()))

        # Check if the message exists on broker 1
        response = requests.get(f"{broker1_url}/messages",params={
            "topic": "fault-tolerance",
            "partition": 1,
            "offset": 1,
        })
        assertEqual(response.status_code, 200)
        print("Got message {} from broker 1".format(response.json()["content"]))
        assertEqual(response.json()["content"], "fault-message-2")
              
