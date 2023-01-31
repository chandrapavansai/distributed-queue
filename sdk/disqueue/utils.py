import requests as req

def list_topics(broker: str) -> list[str]:
    """Function to list all the topics
    
    Args:
        broker (str): url of the broker
    Return:
        list[str]: list of topics
    Raises:
        Exception: If the response is not ok
    """
    res = req.get(broker + '/topics')
    if not res.ok:
        raise Exception(message=res.json().message)
    print(res.json())
    return res.json().topics

def create_topic(broker: str, topic: str) -> None:
    """Function to create a topic

    Args:
        broker (str): url of the broker
        topic (str): topic to be created

    Raises:
        Exception: If the response is not ok
    """
    res = req.post(broker + '/topics')
    if not res.ok:
        raise Exception(message=res.json().message)
    print(res.json())
    