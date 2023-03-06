# Consumer CRUD operations
from ..metadata import Client


def consumer_exists(consumer_id, cursor):
    Consumer = Client.get(f"consumer:{consumer_id}")
    return not Consumer is None


def topic_registered(consumer_id, topic, cursor):
    Consumer = Client.get(consumer_id)
    return Consumer.topic == topic


def get_round_robin_parition_consumer(consumer_id, topic, cursor):
    Consumer = Client.get(consumer_id)
    return Consumer.get_partition(topic)


def get_related_broker(topic, parition, cursor):
    pass


def get_broker_ip(broker_num, cursor):
    pass


def topic_exists(topic_name, cursor):
    Topic = Topic.get(topic_name)
    return not Topic is None


def parition_exists(topic, parition, cursor):
    Parition = Parition.get(f"{topic}:{parition}")
    return not Parition is None


def register_consumer(consumer_id, topic, parition, cursor):
    Consumer = Client("consumer", consumer_id)
    Consumer.register_topic(topic, parition)
    pass

# Producer CRUD operations


def producer_exists(producer_id, cursor):
    Producer = Producer.get(producer_id)
    return not Producer is None
    pass


def topic_registered(producer_id, topic, cursor):
    Producer = Producer.get(producer_id)
    return Producer.topic == topic


def get_round_robin_parition_producer(producer_id, topic, cursor):
    Producer = Client("producer", producer_id)
    return Producer.get_partition(topic)


def register_producer(producer_id, topic, parition, cursor):
    Producer = Client("producer", producer_id)
    Producer.register_topic(topic, parition)
    pass
