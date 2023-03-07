import threading
from typing import Dict

from connection import Connection
from sync_queue import SyncQueue
from time import sleep
from topic import Topic


class TopicConsumer:
    FETCH_FREQUENCY = 10

    def __init__(self, topic: Topic, connection: Connection):
        self.topic = topic
        self.connection = connection
        self._stop_threads = False
        self._queue = SyncQueue()
        self._thread = threading.Thread(target=self._worker)
        res = self.connection.post_readonly('/consumer/register', params=topic.dict())
        if not res.ok:
            raise Exception('Error while registering topic')
        self._cons_id = res.json()['consumer_id']
        self._queue = SyncQueue()
        self._thread = threading.Thread(target=self._worker)

    def _fetch_next(self) -> None:

        res = self.connection.get('/consumer/consume', params={**self.topic.dict(), 'consumer_id': self._cons_id})
        if not res.ok:
            raise Exception('Error while getting next message', res.json())
        self._queue.put(res.json()['message'])

    def _worker(self, ) -> None:
        while not self._stop_threads:
            try:
                self._fetch_next()
            finally:
                sleep(1 / self.FETCH_FREQUENCY)

    def get_size(self) -> int:
        size_in_buffer = len(self._queue)
        res = self.connection.get('/size', params={**self.topic.dict(), 'consumer_id': self._cons_id})
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

    def __init__(self, topics: list[Topic], connection: Connection):
        self._consumers: Dict[Topic, TopicConsumer] = dict()
        self.connection = connection

        for topic in topics:
            self.register_topic(topic)

    def register_topic(self, topic: Topic) -> None:
        self._consumers[topic] = TopicConsumer(topic, self.connection)

    def get_next(self, topic: Topic) -> str:
        if topic not in self._consumers:
            self.register_topic(topic)
        return self._consumers[topic].get_next()

    def get_size(self, topic: Topic) -> int:
        if topic not in self._consumers:
            raise Exception('Topic not registered')
        return self._consumers[topic].get_size()
