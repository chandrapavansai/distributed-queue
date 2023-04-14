from raft import Raft
import socket
class Broker:
    def __init__(self,topics,host):
        self.host = host
        # Create a dictionary of Raft objects, one for each partition replica
        self.raft = {}
        for topic,partitionObj in topics.items():
            self.raft[topic] = {}
            for partition,partners in partitionObj.items():
                self.raft[topic][partition] = Raft(self.get_url(partners), self.filter_partners(partners),host, topic, partition)
                self.raft[topic][partition].waitReady()
    
    def get_url(self,partners):
        for partner in partners:
            # If starts with http or not
            # Get ip of partner
            ip = partner.split(':')[1].replace('//','') if partner.startswith('http') else partner.split(':')[0]
            port = partner.split(':')[2] if partner.startswith('http') else partner.split(':')[1]
            if ip == self.host:
                return partner

        print(f"No url found for host {self.host}")
        return None


    def filter_partners(self, partners):
        part = [] # array of other partners
        # Get partners except self
        for partner in partners:
            ip = partner.split(':')[1].replace('//','') if partner.startswith('http') else partner.split(':')[0]
            if ip != self.host:
                part.append(partner)
        return part

    def exists(self, topic, partition):
        return topic in self.raft and partition in self.raft[topic]

    def create_message(self, topic, partition, message, partners):
        # Check if topic exists
        if topic not in self.raft:
            self.raft[topic] = {}
        # Check if partition exists
        if partition not in self.raft[topic]:
            self.raft[topic][partition] = Raft(self.get_url(partners), self.filter_partners(partners),self.host, topic, partition)
            self.raft[topic][partition].waitReady()
        
        # Check status of raft object
        print(f'Raft status: {self.raft[topic][partition].getStatus()}')
        # Check if ready
        print(f'Raft ready: {self.raft[topic][partition].isReady()}')
        # return 'hello'
        # Produce message
        return self.raft[topic][partition].create_message(message)
    
    def get_message(self, topic, partition, offset):
        # Check if topic exists
        if not self.exists(topic, partition):
            return
        return self.raft[topic][partition].get_message(offset)
    
    def get_message_count(self, topic, partition, offset):
        # Check if topic exists
        if not self.exists(topic, partition):
            return
        return self.raft[topic][partition].get_message_count(offset)
    
    def get_leader(self, topic, partition):
        # Check if topic exists
        if not self.exists(topic, partition):
            return
        return self.raft[topic][partition].get_leader()
    
    def create_topic(self, topic, partition, partners):
        # Check if topic exists
        if self.exists(topic, partition):
            return
        self.raft[topic] = {} if topic not in self.raft else self.raft[topic]
        self.raft[topic][partition] = Raft(self.get_url(partners), partners,self.host, topic, partition)
        self.raft[topic][partition].waitReady()
    
    def delete_topic(self, topic, partition):
        # Check if topic exists
        if not self.exists(topic, partition):
            return
        self.raft[topic][partition].clear_queue(0)
    
    def get_free_port(self):
        """
        Starts a socket connection to grab a free port (Involves a race
            condition but will do for now)
        :return: An open port in the system
        """
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.bind(('', 0))
        _, port = tcp.getsockname()
        tcp.close()
        return port

    def __del__(self):
        for topic,partitionObj in self.raft.items():
            for partition,raftObj in partitionObj.items():
                raftObj.destroy()    
    
        
