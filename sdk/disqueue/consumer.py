import requests as requests
import threading
from typing import Dict
from sync_queue import SyncQueue
from time import sleep


class TopicConsumer:
    FETCH_FREQUENCY = 10

    def __init__(self, topic: str, broker: str):
        self.topic = topic
        self.broker = broker
        self._stop_threads = False
        self._queue = SyncQueue()
        self._thread = threading.Thread(target=self._worker)
        res = requests.post(self.broker + '/consumer/register',
                            params={'topic': topic})
        if not res.ok:
            raise Exception('Error while registering topic')
        self._cons_id = res.json()['consumer_id']
        self._queue = SyncQueue()
        self._thread = threading.Thread(target=self._worker)

    def _fetch_next(self) -> None:

        res = requests.get(self.broker + '/consumer/consume',
                           params={'topic': self.topic,
                              'consumer_id': self._cons_id})

        self._queue.put(res.json()['message'])

        # if res.status_code == 404 and res.json()['detail'] == 'Queue is empty':
        #     return
        if not res.ok:
            raise Exception('Error while getting next message', res.json())

    def _worker(self, ) -> None:
        while not self._stop_threads:
            try:
                self._fetch_next()
            finally:
                sleep(1 / self.FETCH_FREQUENCY)

    def get_size(self) -> int:
        size_in_buffer = len(self._queue)
        res = requests.get(self.broker + '/size',
                           params={'topic': self.topic,
                              'consumer_id': self._cons_id})
        if not res.ok:
            raise Exception('Error while getting size')

        return res.json()['size'] + size_in_buffer

    def get_next(self) -> str:
        if self._queue.empty:
            self._fetch_next()
        return self._queue.get()

    def __del__(self):
        self._stop_threads = True
        self._thread.join()


class Consumer:
    FETCH_FREQUENCY = 10

    def __init__(self, topics: list[str], broker: str):
        """Constructor for Consumer class

        Args:
            topics (list[str]): List of topics to be consumed
            broker (str): url of the broker
        """
        # Check if / is present at the end of broker
        if broker[-1] == '/':
            broker = broker[:-1]  # Remove the last character
        self._consumers: Dict[str, TopicConsumer] = dict()
        self.broker = broker

        for topic in topics:
            self.register_topic(topic)

    def register_topic(self, topic: str) -> None:
        self._consumers[topic] = TopicConsumer(topic, self.broker)

    def get_next(self, topic: str) -> str:
        if topic not in self._consumers:
            self.register_topic(topic)
        return self._consumers[topic].get_next()

    def get_size(self, topic: str) -> int:
        return self._consumers[topic].get_size()
