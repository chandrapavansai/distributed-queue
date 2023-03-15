from connection import Connection
from consumer import Consumer
from producer import Producer
from topic import Topic

topic = Topic(topic_name='T1', partition=1)
connection = Connection(["https://localhost:8090"])

if '__name__' == '__main__':
    producer = Producer(topics=[topic], connection=connection)