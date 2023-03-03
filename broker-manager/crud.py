from database import db


def topic_exists(topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Topic WHERE name = %s", (topic,))
    return cursor.fetchone()[0] > 0


def insert_parition_broker(topic, parition, broker_id, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("INSERT INTO BrokerParition (topic_name, parition_num, broker_id) VALUES (%s, %s, %s)",
                   (topic, parition, broker_id))


def create_topic(topic_name, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("INSERT INTO Topic (name) VALUES (%s)", (topic_name,))


def partition_exists(topic, parition, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM BrokerParition WHERE topic_name = %s AND parition_num = %s", (topic, parition))
    return cursor.fetchone()[0] > 0


def get_related_broker(topic, parition, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT broker_id FROM BrokerParition WHERE topic_name = %s AND parition_num = %s", (topic, parition))
    return cursor.fetchone()[0]


def get_broker_ip(broker_id, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT ip FROM Broker WHERE id = %s", (broker_id,))
    return cursor.fetchone()[0]


def get_topics_paritions(topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT parition_num FROM BrokerParition WHERE topic_name = %s", (topic,))
    return cursor.fetchall()


def get_topics(cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT name FROM Topic")
    return [topic[0] for topic in cursor.fetchall()]


def get_alive_managers(cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT ip FROM Manager WHERE alive = 1")
    return [manager[0] for manager in cursor.fetchall()]


def get_consumer_topic_parition(client_id, topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute(
        "SELECT parition_num FROM TopicPos WHERE client_id = %s AND topic_name = %s", (client_id, topic))
    return cursor.fetchone()[0]


def set_consumer_topic_parition(client_id, topic, parition, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("UPDATE TopicPos SET parition_num = %s WHERE client_id = %s AND topic_name = %s",
                   (parition, client_id, topic))
