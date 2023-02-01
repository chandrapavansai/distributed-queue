# End to end testing as suggested in piazza

import os
import sys

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Add disqueue to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Imports
from disqueue import Producer, Consumer
import requests
import threading
import random
import time

# Global variables
prodExited = 0

PORT = "8080"

def run_producer(file_name, topics):
    prefix = "P-" + file_name[-5]
    # Create producer
    producer = Producer(topics=topics, broker="http://localhost:" + PORT)

    # Read log from filename
    with open(file_name, "r") as f:
        log = f.read()
        for line in log.splitlines():
            message_sent = False
            while not message_sent:
            # Extract info
                message = line.split("\t")[1]
                topic = line.split("\t")[-1]

                # Send message
                try:
                    producer.send_message(topic=topic, message=message)
                    print(prefix,f"\tSent message {message} to topic {topic}")
                    message_sent = True
                except requests.exceptions.ConnectionError:
                    # Don't show messages for retry errors
                    pass
                except Exception as e:
                    print(prefix,"\t",e)
                time.sleep(random.random())
        
    print(prefix,"\tProducer Completed Producing Messages")
    time.sleep(60)
    # Giving the consumers a 60 seconds time to consume all the messages
    global prodExited
    prodExited += 1

def run_consumer(name,topics):
    prefix = "C-" + name
    # Create consumer
    consumer = Consumer(topics=topics, broker="http://localhost:" + PORT)
    
    # Consume messages
    while True:
        global prodExited
        if prodExited >= 5:
            break

        for topic in topics:
            try:
                topic_size = consumer.get_size(topic=topic)
                if not topic_size:
                    continue
                message = consumer.get_next(topic=topic)
                print(prefix,f"\tConsuming message from topic {topic} : {message}")
            except requests.exceptions.ConnectionError:
                # Don't show messages for retry errors
                # Sleep for a bounded amount of time to avoid spamming the server
                time.sleep(random.random())
                pass
            except Exception as e:
                print(prefix,"\t",e)


if __name__ == '__main__':


    # Create Producer threads

    print("Starting threads")
    producer_threads = []
    producer_threads.append(
        threading.Thread(target=run_producer, args=("./test_asgn1/producer_1.txt", ["T-1", "T-2", "T-3"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_2.txt", ["T-1", "T-3"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_3.txt", ["T-1"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_4.txt", ["T-2"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_5.txt", ["T-2"])))

    # producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/test.txt", ["T-2"])))


    # Create Consumer thread
    consumer_threads = []
    consumer_threads.append(threading.Thread(target=run_consumer, args=('1',["T-1", "T-2", "T-3"])))
    consumer_threads.append(threading.Thread(target=run_consumer, args=('2',["T-1", "T-3"])))
    consumer_threads.append(threading.Thread(target=run_consumer, args=('3',["T-1", "T-3"])))
    

    # Start threads
    for thread in producer_threads:
        thread.start()

    for thread in consumer_threads:
        thread.start()

    # Join threads
    for thread in producer_threads:
        thread.join()

    for thread in consumer_threads:
        thread.join()

    # while prodExited >= 5:
    #     time.sleep(1)
    #     for thread in threading.enumerate():
    #         if thread != threading.main_thread():
    #             thread.kill()
    #     sys.exit(0)


    
    

    

