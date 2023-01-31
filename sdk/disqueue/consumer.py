import requests as req


class Consumer:
    def __init__(self, topics: list[str], broker: str):
        self.broker = broker
        self.topic_cons_ids = dict()
        for topic in topics:
            self.register_topic(topic)

    def register_topic(self, topic: str) -> None:
        res = req.post(self.broker + '/consumer/register',
                       json={'topic': topic})
        if not res.ok:
            raise Exception(message=res.json().message)
        self.topic_cons_ids[topic] = res.json()['consumer_id']
        print(res.json())

    def get_next(self, topic: str) -> str:
        res = req.get(self.broker + '/consumer/consume',
                       json={'topic': topic,
                             'consumer_id': self.topic_cons_ids[topic]})
        if not res.ok:
            raise Exception(message=res.json().message)
        print(res.json())
        return res.json().message
    
    def get_size(self, topic: str) -> int:
        res = req.get(self.broker + '/size',
                       json={'topic': topic,
                             'consumer_id': self.topic_cons_ids[topic]})
        if not res.ok:
            raise Exception(message=res.json().message)
        print(res.json())
        return res.json().size

    ...
