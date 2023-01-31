import requests as req

def list_topics(broker: str) -> list[str]:
    res = req.get(broker + '/topics')
    if not res.ok:
        raise Exception(message=res.json().message)
    print(res.json())
    return res.json().topics

def create_topic(broker: str, topic: str) -> None:
    res = req.post(broker + '/topics')
    if not res.ok:
        raise Exception(message=res.json().message)
    print(res.json())
    