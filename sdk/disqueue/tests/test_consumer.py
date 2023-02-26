import unittest
from unittest.mock import MagicMock, patch

from disqueue import Consumer


class TestConsumer(unittest.TestCase):
    @patch('requests.post')
    def test_register_topic_success(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "consumer_id": 129,
            'message': "success"
        }

        mock_requests.return_value = mock_response

        consumer = Consumer(topics=[], broker="localhost")
        consumer.register_topic('ravi')
        self.assertEqual(consumer.topic_cons_ids['ravi'], 129)


    @patch('requests.post')
    def test_register_topic_success(self, mock_requests):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "consumer_id": 1,
            'message': "success"
        }

        mock_requests.return_value = mock_response

        consumer = Consumer(topics=[], broker="localhost")
        consumer.register_topic('ravi')