from redis_om import HashModel , Field

class TopicPartitions(HashModel):
    topic: str = Field(index = True)
    partitions: list[int] = Field(index = False)

    
class BrokerTopicPartitions(HashModel):
    topic: str = Field(index = True)
    broker_id: str = Field(index = True)
    partitions: list[int] = Field(index = False)

