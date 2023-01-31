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


def topic_exists(topic, cursor=None):
    if cursor is None:
        cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM Topic WHERE name = %s", (topic,))
    return cursor.fetchone()[0] > 0
