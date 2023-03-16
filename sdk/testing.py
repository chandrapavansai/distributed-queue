from disqueue import Connection, Consumer, Producer, Topic, list_topics, create_topic
from time import sleep
from uuid import uuid4


def main():
    with Connection(['http://localhost:8091/']) as connection:
        print('connection created')
        topic_name = uuid4()
        create_topic(topic_name, partitions=5, connection=connection)
        with Producer([Topic(topic_name, 1)], connection) as producer:
            # print('producer created')
            # producer.register_topic(Topic(topic_name, 1))
            print('producer registered')
            producer.send_message(Topic(topic_name, 1), 'test message')
            print('message sent')

        with Consumer([Topic(topic_name, 1)], connection=connection) as consumer:
            print('consumer created')
            while True:
                print("Consumer get size")
                print("-->", consumer.get_size(Topic(topic_name, 1)))
                sleep(1)


if __name__ == '__main__':
    main()
