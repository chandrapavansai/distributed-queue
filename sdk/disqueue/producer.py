from connection import Connection
from typing import Dict


class TopicProducer:
    def __init__(self, topic: str, connection: Connection):
        self.topic = topic
        self.connection = connection

        res = connection.post('/producer/register', params={'topic': topic})
        if not res.ok:
            raise Exception('Error while registering topic')
        self._prod_id = res.json()['producer_id']

    def send_message(self, message: str):
        res = self.connection.post('/producer/produce',
                                   params={'topic': self.topic, 'producer_id': self._prod_id, 'message': message})
        if not res.ok:
            raise Exception('Error while sending message', res.json())


class Producer:
    def __init__(self, topics: list[str], connection: Connection):
        """Constructor for Producer class   

        Args:
            topics (list[str]): List of topics to be produced
            connection (Connection): Connection to the broker manager
        """
        # Check if / is present at the end of broker

        self.connection = connection
        self._producers: Dict[str, TopicProducer] = dict()
        for topic in topics:
            self.register_topic(topic)

    def register_topic(self, topic: str):
        """Function to register a topic

        Args:
            topic (str): topic to be registered

        Raises:
            Exception: If the response is not ok
        """
        self._producers[topic] = TopicProducer(topic, self.connection)

    def send_message(self, topic: str, message: str):
        """Function to send a message to a topic

        Args:
            topic (str): topic to be produced
            message (str): message to be sent

        Raises:
            Exception: If the response is not ok
            Exception: If the topic is not registered
        """
        if topic not in self._producers:
            raise Exception('Topic not registered')
        self._producers[topic].send_message(message)