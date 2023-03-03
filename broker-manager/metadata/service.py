from .database import Database

class Service(Database):
    def __init__(self,service_name,service_ip):
        self.service_name = service_name
        self.service_ip = service_ip
        self.id = self.service_name + ":" + self.service_ip

        # Reinitialize service
        self.db.delete(self.id)
        self.set_live()

    def is_live(self):
        # Get service with given ip
        live = self.db.hmget(self.id,["live"])[0].decode("utf-8")
        if live == "1":
            return True
        return False

    def set_live(self):
        self.db.hset(self.id,"live",1)

    def set_dead(self):
        self.db.hset(self.id,"live",0)

    def get_id(self):
        return self.id
    
