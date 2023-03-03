# Import libraries from metadata.py
from metadata import Client,Database
if __name__ == "__main__":
    # Flush DB
    # Test code
    # Create client
    Database.db.flushdb()
    client = Client("client","1")

    db = Client.db
    # FLushDB
    print(db.hgetall("client:1"))

    print(client.get_topic_partition())

    # Add topic_partition
    client.add_topic_partition("topic1",0)
    client.add_topic_partition("topic1",1)

    print(client.get_topic_partition())

    # Get topic_pos
    print(client.get_partition("topic1"))

    # Update topic_pos
    client.update_topic_pos("topic1")

    print(client.get_partition("topic1"))


    
    
