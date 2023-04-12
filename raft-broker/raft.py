from pysyncobj import SyncObj, SyncObjConf, replicated_sync
class Raft(SyncObj):
    def __init__(self, self_node, partner_nodes,host,topic, partition):
        # Creating raft inst
        print(f'Creating Raft for {topic} {partition}...')
        print(f'Self node: {self_node}')
        print(f'Partner nodes: {partner_nodes}')
        port = self_node.split(':')[1]
        cfg = SyncObjConf(dynamicMembershipChange=False, journalFile=f'.journals/journal_{port}_{host}_{topic}_{partition}.journal',
                          logLevel='DEBUG'
                          )
        super().__init__(self_node, partner_nodes, conf=cfg)

        self.topic = topic
        self.partition = partition
        self.__queue= []

    # @property
    # def topic(self):
    #     return self.__topic
    
    # @property
    # def partition(self):
    #     return self.__partition
    
    @replicated_sync
    def create_message(self,message):
        return self.__queue.append(message)
    
    @replicated_sync
    def get_message(self, offset):
        if offset >= len(self.__queue):
            return None
        return self.__queue[offset]

    @replicated_sync
    def get_message_count(self, offset):
        if offset >= len(self.__queue):
            return None
        return len(self.__queue) - offset
    
    @replicated_sync
    def clear_queue(self, offset):
        self.__queue = []
    
    def get_leader(self):
        return self.getLeader()

    def waitReady(self):
        print(f'Waiting for Raft to be ready for {self.topic} {self.partition}...')
        super().waitReady()
        print(f'Raft is ready for {self.topic} {self.partition}!')
        return True
    
    
