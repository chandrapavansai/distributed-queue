from disqueue import Connection, Consumer, Producer, Topic, list_topics, create_topic
from time import sleep


def main():
    with Connection(['http://localhost:8091/']) as connection:
        print('connection created')
        create_topic('test1', partitions=5, connection=connection)
        topic = Topic('test1', 1)
        with Producer([topic], connection) as producer:
            print('producer created')
            producer.send_message(topic, 'test message')
            print('message sent')

if __name__ == '__main__':
    main()

