from .partition import Partition
import json
class Broker(Partition):
    def __init__(self,service_name,service_ip):
        super().__init__(service_name,service_ip)

        # Add empty topic_partition
        self.set_topic_partition({})

    def get_topic_partition(self):
        return json.loads(self.db.hget(self.id, ["topic_partition"]))

    def set_topic_partition(self, topic_partition):
        self.db.hset(self.id,"topic_partition",json.dumps(topic_partition))
    
    def add_topic_partition(self, topic, partition):
        # Get topic_partition
        topic_partition = self.get_topic_partition()
        # Add topic
        topic_partition[topic].append(partition)
        # Update topic_partition
        self.set_topic_partition(topic_partition)
