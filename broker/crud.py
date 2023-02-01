from database import db


def create_topic(topic_name, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("INSERT INTO Topic (name) VALUES (%s)", (topic_name,))


def list_topics(cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT name FROM Topic")
    return [topic[0] for topic in cursor.fetchall()]


def register_producer(producer, topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("INSERT INTO Producer_Topic (producer_id, topic_name) VALUES (%s, %s)", (producer, topic))


def register_consumer(consumer_id, topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("INSERT INTO Consumer_Topic (consumer_id, topic_name) VALUES (%s, %s)", (consumer_id, topic))


def enqueue_message(topic, message, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("INSERT INTO Queue (topic_name, message) VALUES (%s, %s)", (topic, message))


def get_producer_topic(producer_id, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT topic_name FROM Producer_Topic WHERE producer_id = %s", (producer_id,))
    output = cursor.fetchone()
    return None if output is None else output[0]


def get_consumer_topic(consumer_id, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT topic_name FROM Consumer_Topic WHERE consumer_id = %s", (consumer_id,))
    output = cursor.fetchone()
    return None if output is None else output[0]


def topic_exists(topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Topic WHERE name = %s", (topic,))
    return cursor.fetchone()[0] > 0


def consumer_exists(consumer_id, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Consumer_Topic WHERE consumer_id = %s", (consumer_id,))
    return cursor.fetchone()[0] > 0


def producer_exists(producer_id, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Producer_Topic WHERE producer_id = %s", (producer_id,))
    return cursor.fetchone()[0] > 0
