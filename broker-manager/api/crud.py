# Consumer CRUD operations
from database import db


# Tested
def consumer_exists(consumer_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Consumer WHERE consumer_id = %s", (consumer_id,))
    return cursor.fetchone()[0] > 0


# Tested
def get_topics(cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT DISTINCT topic_name FROM Topic")
    return [topic[0] for topic in cursor.fetchall()]


# Tested
def get_partitions(topic, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT partition_id FROM Topic WHERE topic_name = %s", (topic,))
    return [partition[0] for partition in cursor.fetchall()]


def get_partition_count(topic, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s", (topic,))
    return cursor.fetchone()[0]


# Tested
def topic_registered_consumer(consumer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Consumer WHERE consumer_id = %s AND topic_name = %s", (consumer_id, topic))
    return cursor.fetchone()[0] > 0


# Partially Tested
def get_round_robin_partition_consumer(consumer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()

    cursor.execute(
        "SELECT partition_id FROM Consumer WHERE consumer_id = %s", (consumer_id, ))
    partition = cursor.fetchone()[0]

    cursor.execute(
        "SELECT is_round_robin FROM Consumer WHERE consumer_id = %s", (consumer_id, ))
    is_round_robin = cursor.fetchone()[0]

    if not is_round_robin:
        return partition

    original_partition = partition

    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s", (topic, ))
    partition_count = cursor.fetchone()[0]

    for i in range(partition_count):
        current_partition = (partition + i) % partition_count

        # If entry not present in ConsumerPartition, create it
        cursor.execute(
            "SELECT COUNT(*) FROM ConsumerPartition WHERE consumer_id = %s AND partition_id = %s", (consumer_id, current_partition))
        entry = cursor.fetchone()[0]
        if not entry:
            cursor.execute(
                "INSERT INTO ConsumerPartition (consumer_id, partition_id) VALUES (%s, %s)", (consumer_id, current_partition))

        # 0-indexed, current position to read from
        offset = get_offset(consumer_id, current_partition, cursor)
        size = get_size(topic, current_partition, cursor)  # size is 1-indexed

        if offset < size:
            new_partition = (current_partition + 1) % partition_count
            cursor.execute(
                "UPDATE Consumer SET partition_id = %s WHERE consumer_id = %s", (new_partition, consumer_id))
            return current_partition

    # All partitions are empty, so just move forward
    new_partition = (partition + 1) % partition_count
    cursor.execute(
        "UPDATE Consumer SET partition_id = %s WHERE consumer_id = %s", (new_partition, consumer_id))
    return original_partition


# Tested
def get_offset(consumer_id, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT offset_val FROM ConsumerPartition WHERE consumer_id = %s AND partition_id = %s", (consumer_id, partition))
    offset = cursor.fetchone()
    if offset is None:
        return 0
    return offset[0]


def increment_size(topic, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    print("Updating size")
    cursor.execute(
        "UPDATE Topic SET size = size + 1 WHERE topic_name = %s AND partition_id = %s", (topic, partition))


def increment_offset(consumer_id, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "UPDATE ConsumerPartition SET offset_val = offset_val + 1 WHERE consumer_id = %s AND partition_id = %s", (consumer_id, partition))


def get_size(topic, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT size FROM Topic WHERE topic_name = %s AND partition_id = %s", (topic, partition))
    return cursor.fetchone()[0]


# Tested
def get_related_broker(topic, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT broker_id FROM Topic WHERE topic_name = %s AND partition_id = %s", (topic, partition))
    return cursor.fetchone()[0]


# Tested
def get_broker_ip(broker_num, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT ip_addr FROM Broker WHERE broker_id = %s", (broker_num,))
    return cursor.fetchone()[0]


# Tested
def topic_exists(topic_name, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s", (topic_name,))
    return cursor.fetchone()[0] > 0


# Tested
def partition_exists(topic, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s AND partition_id = %s", (topic, partition))
    return cursor.fetchone()[0] > 0


# Tested
def register_consumer(consumer_id, topic, partition, is_round_robin, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Consumer (consumer_id, topic_name, partition_id, is_round_robin) \
            VALUES (%s, %s, %s, %s)", (consumer_id, topic, partition, is_round_robin))
    cursor.execute(
        "INSERT INTO ConsumerPartition (consumer_id, partition_id, offset_val) \
            VALUES (%s, %s, %s)", (consumer_id, partition, 0))
    pass


# Tested
def producer_exists(producer_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Producer WHERE producer_id = %s", (producer_id,))
    return cursor.fetchone()[0] > 0


# Tested
def topic_registered_producer(producer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Producer WHERE producer_id = %s AND topic_name = %s", (producer_id, topic))
    return cursor.fetchone()[0] > 0


# Partially Tested
def get_round_robin_partition_producer(producer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()

    cursor.execute(
        "SELECT partition_id FROM Producer WHERE producer_id = %s", (producer_id, ))
    partition = cursor.fetchone()[0]

    cursor.execute(
        "SELECT is_round_robin FROM Producer WHERE producer_id = %s", (producer_id, ))
    is_round_robin = cursor.fetchone()[0]

    if not is_round_robin:
        return partition

    original_partition = partition

    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s", (topic,))
    partition_count = cursor.fetchone()[0]
    new_partition = (partition + 1) % partition_count

    # Set the new partition
    cursor.execute(
        "UPDATE Producer SET partition_id = %s WHERE producer_id = %s", (new_partition, producer_id))

    return original_partition


# Tested
def register_producer(producer_id, topic, partition, is_round_robin, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Producer (producer_id, topic_name, partition_id, is_round_robin) \
            VALUES (%s, %s, %s, %s)", (producer_id, topic, partition, is_round_robin))
    pass


# Not Tested
def get_alive_managers(cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT ip FROM Manager WHERE is_alive = 1")
    return cursor.fetchall()


def set_consumer_hearbeat(consumer_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "UPDATE Consumer SET last_timestamp = NOW() WHERE consumer_id = %s", (consumer_id, ))
    pass


def get_consumer_hearbeat(consumer_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT last_timestamp FROM Consumer WHERE consumer_id = %s", (consumer_id, ))
    return cursor.fetchone()[0]


def set_producer_hearbeat(producer_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "UPDATE Producer SET last_timestamp = NOW() WHERE producer_id = %s", (producer_id, ))
    pass


def get_producer_hearbeat(producer_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT last_timestamp FROM Producer WHERE producer_id = %s", (producer_id, ))
    return cursor.fetchone()[0]


def get_broker_partitions(broker_id, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT topic_name, partition_id FROM Topic WHERE broker_id = %s", (broker_id,))
    return cursor.fetchall()


# Tested
def set_partition_broker(broker_id, topic, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Topic (broker_id, topic_name, partition_id) VALUES (%s, %s, %s)", (broker_id, topic, partition))
    pass


# Tested
def update_partition_broker(broker_id, topic, partition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "UPDATE Topic SET broker_id = %s WHERE topic_name = %s AND partition_id = %s", (broker_id, topic, partition))
    pass


def get_broker_id(ip: str, cursor):
    """
    Utility function to get the broker id from the ip address
    returns the broker id
    """
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT broker_id FROM Broker WHERE ip_addr = %s", (ip,))
    return cursor.fetchone()[0]
