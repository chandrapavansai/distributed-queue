from .service import Service
import json
import logging

# Print logging info
logging.basicConfig(level=logging.INFO)

class Client(Service):
    def __init__(self,service_name,service_ip):
        super().__init__(service_name,service_ip)

        # Add empty topic_pos and topic_partition
        self.set_topic_pos({})
        self.set_topic_partition({})
    
    def get_topic_pos(self) -> dict:
        return json.loads(self.db.hget(self.id, "topic_pos").decode("utf-8"))

    def get_topic_partition(self):
        return json.loads(self.db.hget(self.id, "topic_partition").decode("utf-8"))

    def set_topic_partition(self, topic_partition):
        self.db.hset(self.id,"topic_partition",json.dumps(topic_partition))

    def set_topic_pos(self, topic_pos):
        self.db.hset(self.id,"topic_pos",json.dumps(topic_pos))
    
    def add_topic_partition(self, topic, partition):
        # Get topic_partition
        topic_partition = self.get_topic_partition()

        if topic not in topic_partition: # Check if topic_partition has topic
            topic_partition[topic] = [partition]
        else: # Add topic
            topic_partition[topic].append(partition)
        
        # Update topic_partition
        self.set_topic_partition(topic_partition)
    
    def update_topic_pos(self, topic):
        # Get topic_partition
        topic_partition = self.get_topic_partition()
        num_partitions = len(topic_partition[topic])
        # Get topic_pos
        topic_pos = self.get_topic_pos()
        
        logging.info("Updating topic_pos for topic {}".format(topic))
        logging.info("topic_pos: {}".format(topic_pos))

        if topic not in topic_pos: # Check if topic_pos has topic
            topic_pos[topic] = 0
        else: # Update topic_pos
            logging.info("Updating topic_pos for topic {}".format(topic))
            topic_pos[topic] += 1
        if topic_pos[topic] >= num_partitions:
            topic_pos[topic] = 0
        
        # Update topic_pos
        self.set_topic_pos(topic_pos)

    def get_partition(self, topic):
        # Get topic_partition
        topic_partition = self.get_topic_partition()

        # Check if topic partition exists
        if topic not in topic_partition:
            raise Exception("Partition {} does not exist in topic {}".format(topic_partition,topic))

        # Get topic_pos
        topic_pos = self.get_topic_pos()

        if topic not in topic_pos: # Check if topic_pos has topic
            topic_pos[topic] = 0
            # Update topic_pos
            self.set_topic_pos(topic_pos)

        # Get partition
        partition = topic_partition[topic][topic_pos[topic]]
        return partition

        