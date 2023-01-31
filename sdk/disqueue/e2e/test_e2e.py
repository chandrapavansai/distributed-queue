# End to end testing as suggested in piazza

import os
import sys
# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Add disqueue to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Imports
from disqueue import Producer, Consumer
import threading
import random
import time

PORT = 8000

def run_producer(file_name, topics):
    # Create producer
    producer = Producer(topics=topics, broker="http://localhost:8000")

    # Read log from filename
    with open(file_name, "r") as f:
        log = f.read()
        for line in log.splitlines():
            # Extract info
            message = line.split("\t")[1]
            topic = line.split("\t")[-1]

            # Send message
            print("P " + file_name[-5])
            try:
                producer.send_message(topic=topic, message=message)
                print(f"\tSent message {message} to topic {topic}")
            except Exception as e:
                print(e)
            time.sleep(random.randint(1, 2))

def run_consumer(name,topics):
    # Create consumer
    consumer = Consumer(topics=topics, broker="http://localhost:8000")
    kill = False
    # Kill thread after 10 seconds
    def kill_thread():
        # Kill thread
        global kill
        kill = True
        print("Killing thread")
    threading.Timer(1, kill_thread).start()

    # Consume messages
    while not kill:
        cnt = 0
        for topic in topics:
            if not consumer.get_size(topic=topic):
                cnt = cnt + 1
                continue
            print("C " + name)
            try:
                message = consumer.get_next(topic=topic)
                print(f"\tConsuming message from topic {topic} : {message}")
            except Exception as e:
                print(e)
        if cnt == len(topics):
            break

if __name__ == '__main__':

    # Kill all threads on ctrl+c
    def signal_handler(sig, frame):
        print("Killing all threads")
        for thread in threading.enumerate():
            if thread != threading.main_thread():
                thread.kill()
        sys.exit(0)
    
    import signal
    signal.signal(signal.SIGINT, signal_handler)

    # Create Producer threads
    producer_threads = []
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_1.txt", ["T-1", "T-2", "T-3"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_2.txt", ["T-1", "T-3"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_3.txt", ["T-1"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_4.txt", ["T-2"])))
    producer_threads.append(threading.Thread(target=run_producer, args=("./test_asgn1/producer_5.txt", ["T-2"])))

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


    
    

    

