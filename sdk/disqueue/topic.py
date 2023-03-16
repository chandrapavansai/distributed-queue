from typing import Optional


class Topic:
    def __init__(self, topic_name: str, partition: Optional[int] = None):
        self.topic_name = topic_name
        self.partition = partition

    def __eq__(self, other):
        return self.topic_name == other.topic_name and self.partition == other.partition

    def dict(self):
        if self.partition is not None:
            return {
                'topic': self.topic_name,
                'partition': self.partition
            }
        else:
            return {
                'topic': self.topic_name
            }
