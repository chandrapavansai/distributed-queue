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
    cursor.execute("SELECT topic_name FROM Topic")
    return [topic[0] for topic in cursor.fetchall()]


# Tested
def get_paritions(topic, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT parition_id FROM Topic WHERE topic_name = %s", (topic,))
    return [parition[0] for parition in cursor.fetchall()]


# Tested
def topic_registered_consumer(consumer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Consumer WHERE consumer_id = %s AND topic_name = %s", (consumer_id, topic))
    return cursor.fetchone()[0] > 0


# Partially Tested
def get_round_robin_parition_consumer(consumer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()

    cursor.execute(
        "SELECT parition_id FROM Consumer WHERE consumer_id = %s", (consumer_id, ))
    parition = cursor.fetchone()[0]

    cursor.execute(
        "SELECT is_round_robin FROM Consumer WHERE consumer_id = %s", (consumer_id, ))
    is_round_robin = cursor.fetchone()[0]

    if not is_round_robin:
        return parition

    original_parition = parition

    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s", (topic, ))
    parition_count = cursor.fetchone()[0]

    parition = (parition + 1) % parition_count

    # Set the new parition
    cursor.execute(
        "UPDATE ConsumerPartition SET parition_id = %s WHERE consumer_id = %s", (parition, consumer_id))

    return original_parition


# Tested
def get_offset(consumer_id, parition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT offset_val FROM ConsumerPartition WHERE consumer_id = %s AND parition_id = %s", (consumer_id, parition))
    return cursor.fetchone()[0]


# Tested
def get_related_broker(topic, parition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT broker_id FROM Topic WHERE topic_name = %s AND parition_id = %s", (topic, parition))
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
def parition_exists(topic, parition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s AND parition_id = %s", (topic, parition))
    return cursor.fetchone()[0] > 0


# Tested
def register_consumer(consumer_id, topic, parition, is_round_robin, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Consumer (consumer_id, topic_name, parition_id, is_round_robin) \
            VALUES (%s, %s, %s, %s)", (consumer_id, topic, parition, is_round_robin))
    cursor.execute(
        "INSERT INTO ConsumerPartition (consumer_id, parition_id, offset_val) \
            VALUES (%s, %s, %s)", (consumer_id, parition, 0))
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
def get_round_robin_parition_producer(producer_id, topic, cursor):
    if cursor is None:
        cursor = db.cursor()

    cursor.execute(
        "SELECT parition_id FROM Producer WHERE producer_id = %s", (producer_id, ))
    parition = cursor.fetchone()[0]

    cursor.execute(
        "SELECT is_round_robin FROM Producer WHERE producer_id = %s", (producer_id, ))
    is_round_robin = cursor.fetchone()[0]

    if not is_round_robin:
        return parition

    cursor.execute(
        "SELECT COUNT(*) FROM Topic WHERE topic_name = %s", (topic,))
    parition_count = cursor.fetchone()[0]
    parition = (parition + 1) % parition_count

    # Set the new parition
    cursor.execute(
        "UPDATE Producer SET parition_id = %s WHERE producer_id = %s", (parition, producer_id))

    return parition


# Tested
def register_producer(producer_id, topic, parition, is_round_robin, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Producer (producer_id, topic_name, parition_id, is_round_robin) \
            VALUES (%s, %s, %s, %s)", (producer_id, topic, parition, is_round_robin))
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
        "SELECT topic_name, parition_id FROM Topic WHERE broker_id = %s", (broker_id,))
    return cursor.fetchall()


# Tested
def set_partition_broker(broker_id, topic, parition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Topic (broker_id, topic_name, parition_id) VALUES (%s, %s, %s)", (broker_id, topic, parition))
    pass


# Tested
def update_partition_broker(broker_id, topic, parition, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "UPDATE Topic SET broker_id = %s WHERE topic_name = %s AND parition_id = %s", (broker_id, topic, parition))
    pass


# Tested
def create_broker(broker_id, ip, cursor):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "INSERT INTO Broker (broker_id, ip_addr) VALUES (%s, %s)", (broker_id, ip))
    pass
