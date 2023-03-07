import datetime

from redis_om import JsonModel, Field

class Producer(JsonModel):
    live: bool = Field(index=False, default=True)
    last_heartbeat: datetime.datetime = Field(index=False, default=datetime.datetime.now())

    def update_heartbeat(self):
        self.last_heartbeat = datetime.datetime.now()
        self.save()
