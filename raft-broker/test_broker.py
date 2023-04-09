import unittest
import requests
import time


class TestBroker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.broker1_url = "http://localhost:8000"
        cls.broker2_url = "http://localhost:8002"


    def test_ping(self):
        response = requests.get(f"{self.broker1_url}/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "pong"})

        # Test 2nd broker
        response = requests.get(f"{self.broker2_url}/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"message": "pong"})

    def test_create_message(self):
        # Delete the topic-partition if it exists
        response = requests.delete(f"{self.broker1_url}/messages", params={
            "topic": "test",
            "partition": 1,
        })
        self.assertEqual(response.status_code, 200)
        
        # Create a message on broker 1
        response = requests.post(f"{self.broker1_url}/messages", params={
            "topic": "test",
            "partition": 1,
            "content": "test message",
            "partners": "localhost:8002"
        })
        self.assertEqual(response.status_code, 201)

        # offset = response.json()["id"]
        # Check if the message exists on broker 2
        response = requests.get(f"{self.broker2_url}/messages",params={
            "topic": "test",
            "partition": 1,
            "offset": 11,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["content"], "test message")

    def test_get_message_count(self):
        response = requests.get(f"{self.broker1_url}/messages/count",params={
            "topic": "test",
            "partition": 1,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), 1)

if __name__ == '__main__':
    unittest.main()
