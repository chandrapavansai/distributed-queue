from raft import Raft
import socket
import threading
class Broker:
    def __init__(self,topics,host):
        self.host = host

        port_lock = threading.Lock()

        # Maintain locks for each topic and partition
        self.locks = {}
        for topic,partitionObj in topics.items():
            self.locks[topic] = {}
            for partition,partners in partitionObj.items():
                self.locks[topic][partition] = threading.Lock()

        # Create a dictionary of Raft objects, one for each partition replica
        self.raft = {}
        for topic,partitionObj in topics.items():
            self.raft[topic] = {}
            for partition,partners in partitionObj.items():
                with self.locks[topic][partition]:
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

        if not self.exists(topic,partition):
            return
        # Check if lock exists
        if topic not in self.locks or partition not in self.locks[topic]:
            return
        
        with self.locks[topic][partition]:
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
        with self.locks[topic][partition]:
            return self.raft[topic][partition].get_message(offset)
    
    def get_message_count(self, topic, partition, offset):
        # Check if topic exists
        if not self.exists(topic, partition):
            return
        with self.locks[topic][partition]:
            return self.raft[topic][partition].get_message_count(offset)
    
    def get_leader(self, topic, partition):
        # Check if topic exists
        if not self.exists(topic, partition):
            return
        with self.locks[topic][partition]:
            return self.raft[topic][partition].get_leader()
    
    def create_topic(self, topic, partition, partners):
        # Create lock for topic and partition
        self.locks[topic] = {} if topic not in self.locks else self.locks[topic]
        self.locks[topic][partition] = threading.Lock() if partition not in self.locks[topic] else self.locks[topic][partition]

        with self.locks[topic][partition]:
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
        with self.locks[topic][partition]:
            self.raft[topic][partition].clear_queue(0)
    
    def get_free_port(self):
        """
        Starts a socket connection to grab a free port (Involves a race
            condition but will do for now)
        :return: An open port in the system
        """
        with self.port_lock:
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.bind(('', 0))
            _, port = tcp.getsockname()
            tcp.close()
            return port

    def __del__(self):
        for topic,partitionObj in self.raft.items():
            for partition,raftObj in partitionObj.items():
                raftObj.destroy()
    
        
