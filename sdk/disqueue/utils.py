from connection import Connection


def list_topics(connection: Connection) -> list[str]:
    """Function to list all the topics
    
    Args:
        connection (Connection): Connection to broker manager
    Return:
        list[str]: list of topics
    Raises:
        Exception: If the response is not ok
    """
    res = connection.get('/topics')
    if not res.ok:
        raise Exception('Failed to getch topics list', res.json())
    return res.json()['topics']


def create_topic(topic: str, connection: Connection) -> None:
    """Function to create a topic

    Args:
        topic (str): topic to be created
        connection (Connection): Connection to broker manager

    Raises:
        Exception: If the response is not ok
    """
    res = connection.post('/topics', params={'name': topic})
    if not res.ok:
        raise Exception('Failed to create topic', res.json())
