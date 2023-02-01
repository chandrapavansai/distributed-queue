import requests as req


class Consumer:
    def __init__(self, topics: list[str], broker: str):
        """Constructor for Consumer class

        Args:
            topics (list[str]): List of topics to be consumed
            broker (str): url of the broker
        """
        # Check if / is present at the end of broker
        if broker[-1] == '/':
            broker = broker[:-1] # Remove the last character

        self.broker = broker
        self.topic_cons_ids = dict()
        for topic in topics:
            self.register_topic(topic)

    def register_topic(self, topic: str) -> None:
        """Function to register a topic

        Args:
            topic (str): topic to be registered

        Raises:
            Exception: If the response is not ok
        """
        res = req.post(self.broker + '/consumer/register',
                       params={'topic': topic})
        if not res.ok:
            raise Exception('Error while registering topic')

        self.topic_cons_ids[topic] = res.json()['consumer_id']

    def get_next(self, topic: str) -> str:
        """
        Function to get the next message from the topic
        
        Args:
            topic (str): topic to be consumed
        Return:
            str: message from the topic
        Raises:
            Exception: If the response is not ok
        """
        res = req.get(self.broker + '/consumer/consume',
                       params={'topic': topic,
                             'consumer_id': self.topic_cons_ids[topic]})
        if not res.ok:
            raise Exception('Error while getting next message')

        return res.json()['message']
    
    def get_size(self, topic: str) -> int:
        """Function to get the size of the topic

        Args:
            topic (str): topic to be consumed

        Raises:
            Exception: If the response is not ok

        Returns:
            int: size of the topic
        """
        res = req.get(self.broker + '/size',
                       params={'topic': topic,
                             'consumer_id': self.topic_cons_ids[topic]})
        if not res.ok:
            raise Exception('Error while getting size')

        return res.json()['size']
