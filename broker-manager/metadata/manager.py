from .service import Service

class BrokerManager(Service):
    def __init__(self,service_name,service_ip):
        super().__init__(service_name,service_ip)