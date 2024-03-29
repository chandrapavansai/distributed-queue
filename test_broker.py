import requests
import time

class TestBroker:

    def __init__(self):
        self.broker1_url = "http://localhost:8000"
        self.broker2_url = "http://localhost:8001"
        self.broker3_url = "http://localhost:8002"
    
    def assertEqual(self, a, b):
        assert a == b

    def test_ping(self):
        response = requests.get(f"{self.broker1_url}/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "pong"})

        # Test 2nd broker
        response = requests.get(f"{self.broker2_url}/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "pong"})

        # Test 3rd broker
        response = requests.get(f"{self.broker3_url}/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "pong"})

    def test_create_message(self):
        # Delete the topic-partition if it exists
        response = requests.get(f"{self.broker1_url}/messages/delete", params={
            "topic": "test",
            "partition": 1,
        })
        self.assertEqual(response.status_code, 200)

        # json inside body
        request = requests.post(f"{self.broker1_url}/new", json={
            "topic": "test",
            "partition": 1,
            "partners": ["raft-broker1:9000"]
        })
        self.assertEqual(request.status_code, 200)

        # Create a message on broker 1
        response = requests.post(f"{self.broker1_url}/messages", json={
            "topic": "test",
            "partition": 1,
            "content": "test message",
        })
        self.assertEqual(response.status_code, 201)

        # offset = response.json()["id"]
        # Check if the message exists on broker 2
        response = requests.get(f"{self.broker2_url}/messages",params={
            "topic": "test",
            "partition": 1,
            "offset": 0,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["content"], "test message")

    def test_get_message_count(self):
        # Delete the topic-partition if it exists
        response = requests.get(f"{self.broker1_url}/messages/delete", params={
            "topic": "test",
            "partition": 1,
        })
        self.assertEqual(response.status_code, 200)

        # Create topic partition
        response = requests.post(f"{self.broker1_url}/new", json={
            "topic": "test",
            "partition": 1,
            "partners": ["raft-broker1:9000"]
        })
        self.assertEqual(response.status_code, 200)

        # Create a message on broker 1
        response = requests.post(f"{self.broker1_url}/messages", json={
            "topic": "test",
            "partition": 1,
            "content": "test message",
        })
        self.assertEqual(response.status_code, 201)

        response = requests.get(f"{self.broker1_url}/messages/count",params={
            "topic": "test",
            "partition": 1,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), 1)

    # Create new topic-partition
    def test_create_topic_partition(self):
        # Delete the topic-partition if it exists
        response = requests.get(f"{self.broker1_url}/messages/delete", params={
            "topic": "test",
            "partition": 2,
        })
        self.assertEqual(response.status_code, 200)

        # Create topic partition
        response = requests.post(f"{self.broker1_url}/new", json={
            "topic": "test",
            "partition": 2,
            "partners": ["raft-broker2:9000","raft-broker1:9000"]
        })
        self.assertEqual(response.status_code, 200)

        # Create topic partition
        response = requests.post(f"{self.broker2_url}/new", json={
            "topic": "test",
            "partition": 2,
            "partners": ["raft-broker2:9000","raft-broker1:9000"]
        })
        self.assertEqual(response.status_code, 200)

        time.sleep(1)

        # Create a message on broker 1
        response = requests.post(f"{self.broker1_url}/messages", json={
            "topic": "test",
            "partition": 2,
            "content": "test message",
        })
        self.assertEqual(response.status_code, 201)

        # offset = response.json()["id"]
        # Check if the message exists on broker 2
        response = requests.get(f"{self.broker2_url}/messages",params={
            "topic": "test",
            "partition": 2,
            "offset": 0,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["content"], "test message")

        
if __name__ == '__main__':
    test = TestBroker()

    # Run all methods that start with "test"
    for method in dir(test):
        if method.startswith("test"):
            print("Testing " + method)
            getattr(test, method)()
