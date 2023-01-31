import requests as req


class Producer:
    def __init__(self, topics: list[str], broker: str):
        # Check if / is present at the end of broker
        if broker[-1] == '/':
            broker = broker[:-1]  # Remove the last character

        self.broker = broker
        self.topic_prod_ids = dict()
        for topic in topics:
            self.register_topic(topic)

    def register_topic(self, topic: str) -> int:
        res = req.post(self.broker + '/producer/register',
                       json={'topic': topic})
        if not res.ok:
            raise Exception(message=res.json()['detail'])
        self.topic_prod_ids[topic] = res.json()['producer_id']
        print(res.json())

    def send_message(self, topic: str, message: str):
        if topic not in self.topic_prod_ids:
            raise Exception(message="No topic found")
        res = req.post(self.broker + '/producer/produce',
                       json={'topic': topic,
                             "producer_id": self.topic_prod_ids[topic],
                             "message": message
                             }
                       )
        if not res.ok:
            raise Exception(message=res.json()['edtail'])
        print(res.json())
    ...
