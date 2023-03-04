from .partition import Partition
import json
import logging

# Print logging info
logging.basicConfig(level=logging.INFO)

class Client(Partition):
    def __init__(self,service_name,service_ip):
        super().__init__(service_name,service_ip)

        # Add partition -1
        self.set_partition(-1)

    def register_topic(self, topic_id,partition = -1):
        self.db.hset(self.id,"topic_id",topic_id)
        self.db.hset(self.id,topic_id,partition)
        self.set_partition(partition)
    
    def get_partition(self):
        return int(self.db.hget(self.id, "partition").decode("utf-8"))
    
    def get_topic_id(self):
        return self.db.hget(self.id, "topic_id").decode("utf-8")
    
    def get_curr_partition_round_robin(self, topic_id):
        return int(self.db.hget(self.id, topic_id).decode("utf-8"))

    def set_curr_partition_round_robin(self, topic_id,partition):
        self.db.hset(self.id,topic_id,partition)

    def set_partition(self, partition):
        self.db.hset(self.id,"partition",partition)

    def get_partition(self):
        # Get partition
        partition = self.get_partition()
        # If partition is -1, then get partition from topic
        if partition == -1:
            topic_id = self.get_topic_id()
            partition = self.get_curr_partition_round_robin(topic_id)
            # Update partition
            self.update_partition(topic_id)
        return partition

    def update_partition(self, topic_id):
        # Get current partition index
        partition_idx = self.get_curr_partition_round_robin(topic_id)
        # Get partition list
        partition_list = self.get_gobal_topic_partition(topic_id)

        # Get next partition index
        partition_idx = (partition_idx + 1) % len(partition_list)

        # Update partition
        self.set_curr_partition_round_robin(topic_id,partition_idx)


        