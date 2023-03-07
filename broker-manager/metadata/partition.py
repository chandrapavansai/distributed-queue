from .service import Service
import json
class Partition(Service):
    def __init__(self,service_name,service_ip):
        super().__init__(service_name,service_ip)
        
    def get_global_topic_partition(self, topic_id):
        return json.loads(self.db.hget("topic_partition", topic_id).decode("utf-8"))
    
    def set_global_topic_partition(self, topic_id, partitions):
        self.db.hset("topic_partition",topic_id,json.dumps(partitions))
    
    def add_global_topic_partition(self, topic, partition):
        # Get topic_partition
        topic_partition = self.get_global_topic_partition(topic)

        # Check if topic exists
        if topic not in topic_partition:
            topic_partition[topic] = [partition]
        else:
            # Add topic
            topic_partition[topic].append(partition)

        # Update topic_partition
        self.set_global_topic_partition(topic,topic_partition)