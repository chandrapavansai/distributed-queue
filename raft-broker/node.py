from pysyncobj import Node 

class RaftNode(Node):
    """
    A node intended for communication over TCP/IP. Its id is the network address (host:port).
    """

    def __init__(self,address, **kwargs):
        """
        Initialise the TCPNode
        :param address: network address of the node in the format 'host:port'
        :type address: str
        :param **kwargs: any further information that should be kept about this node
        """
        super(RaftNode, self).__init__(address, **kwargs)
        address = address.split('_')[0]
        self.__address = address
        self.__host, port = address.rsplit(':', 1)
        self.__port = int(port)
        #self.__ip = globalDnsResolver().resolve(self.host)

    @property
    def address(self):
        return self.__address

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def ip(self):
        return globalDnsResolver().resolve(self.__host)

    def __repr__(self):
        v = vars(self)
        filtered = ['_id', '_TCPNode__address', '_TCPNode__host', '_TCPNode__port', '_TCPNode__ip']
        formatted = ['{} = {}'.format(key, repr(v[key])) for key in v if key not in filtered]
        return '{}({}{})'.format(type(self).__name__, repr(self.id), (', ' + ', '.join(formatted)) if len(formatted) else '')
