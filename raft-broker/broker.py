from raft import Raft

class Broker:
    def __init__(self, url, topics,host):
        self.url = url
        self.host = host
        # Create a dictionary of Raft objects, one for each partition replica
        self.raft = {}
        for topic,partitionObj in topics.items():
            self.raft[topic] = {}
            for partition,partners in partitionObj.items():
                self.raft[topic][partition] = Raft(self.url, self.filter_partners(partners),host, topic, partition)
                self.raft[topic][partition].waitReady()

    def filter_partners(self, partners):
        part = []
        # Get partners except self
        for partner in partners:
            print(partner.split(':')[0] , self.host)
            if partner.split(':')[0] != self.host:
                part.append(partner)
        return part

    def create_message(self, topic, partition, message, partners):
        # Check if topic exists
        if topic not in self.raft:
            self.raft[topic] = {}
        # Check if partition exists
        if partition not in self.raft[topic]:
            self.raft[topic][partition] = Raft(self.url, self.filter_partners(partners),self.host, topic, partition)
            self.raft[topic][partition].waitReady()
        
        # Check status of raft object
        print(f'Raft status: {self.raft[topic][partition].getStatus()}')
        # Check if ready
        print(f'Raft ready: {self.raft[topic][partition].isReady()}')
        # return 'hello'
        # Produce message
        return self.raft[topic][partition].create_message(message)
    
    def get_message(self, topic, partition, offset):
        return self.raft[topic][partition].get_message(offset)
    
    def get_message_count(self, topic, partition, offset):
        return self.raft[topic][partition].get_message_count(offset)
    
    def get_leader(self, topic, partition):
        return self.raft[topic][partition].get_leader()
    
    def create_topic(self, topic, partition, partners):
        self.raft[topic] = {}
        self.raft[topic][partition] = Raft(self.url, partners)
        self.raft[topic][partition].waitReady()
    
    def delete_topic(self, topic, partition):
        self.raft[topic][partition].destroy()

    def __del__(self):
        for topic,partitionObj in self.raft.items():
            for partition,raftObj in partitionObj.items():
                raftObj.destroy()    
    
        
