from fastapi.testclient import TestClient
from main import app
from database import clear_db

client = TestClient(app)


def test_ping():
    clear_db()
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"message": "pong"}


def test_initial_topics():
    clear_db()
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {"topics": []}


def test_create_topic():
    clear_db()
    response = client.post("/topics", params={"name": "test"})
    assert response.status_code == 200
    assert response.json() == {"detail": "Topic test created successfully"}


def test_list_topics_after_create_topic():
    clear_db()
    client.post("/topics", params={"name": "test"})
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {"topics": ["test"]}


def test_list_topics_after_create_topics():
    clear_db()
    client.post("/topics", params={"name": "test"})
    client.post("/topics", params={"name": "test2"})
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {"topics": ["test", "test2"]}


def test_create_topic_already_exists():
    clear_db()
    client.post("/topics", params={"name": "test"})
    response = client.post("/topics", params={"name": "test"})

    assert response.status_code == 407
    assert response.json() == {"detail": "Topic already exists"}


def test_register_consumer():
    clear_db()
    client.post("/topics", params={"name": "test"})
    response = client.post("/consumer/register", params={"topic": "test"})
    assert response.status_code == 200
    assert "consumer_id" in response.json().keys()


def test_register_consumer_non_existing_topic():
    clear_db()
    response = client.post("/consumer/register", params={"topic": "test"})
    assert response.status_code == 404
    assert response.json() == {"detail": "Topic not found"}


def test_register_producer():
    clear_db()
    client.post("/topics", params={"name": "test"})
    response = client.post("/producer/register", params={"topic": "test"})
    assert response.status_code == 200
    assert "producer_id" in response.json().keys()


def test_register_producer_new_topic():
    clear_db()
    response = client.post("/producer/register", params={"topic": "test"})
    assert response.status_code == 200
    assert "producer_id" in response.json().keys()
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {"topics": ["test"]}


def test_produce():
    clear_db()
    client.post("/topics", params={"name": "test"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    response = client.post("/producer/produce",
                           params={"topic": "test", "producer_id": producer_id, "message": "hello"})
    assert response.status_code == 200


def test_produce_non_existent_producer_id():
    clear_db()
    client.post("/topics", params={"name": "test"})
    # producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    response = client.post("/producer/produce",
                           params={"topic": "test", "producer_id": "invalid", "message": "hello"})
    assert response.status_code == 404
    assert response.json() == {"detail": "Producer not found"}


def test_produce_wrong_producer_id():
    clear_db()
    client.post("/topics", params={"name": "test"})
    client.post("/topics", params={"name": "test1"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    producer_id_1 = client.post("/producer/register", params={"topic": "test1"}).json()["producer_id"]
    response = client.post("/producer/produce",
                           params={"topic": "test", "producer_id": producer_id_1, "message": "hello"})
    assert response.status_code == 403
    assert response.json() == {"detail": "Producer not registered for this topic"}


def test_produce_non_existent_topic():
    clear_db()
    client.post("/topics", params={"name": "test"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    response = client.post("/producer/produce",
                           params={"topic": "test1", "producer_id": producer_id, "message": "hello"})
    assert response.status_code == 404
    assert response.json() == {"detail": "Topic not found"}


def test_produce_wrong_topic():
    clear_db()
    client.post("/topics", params={"name": "test"})
    client.post("/topics", params={"name": "test1"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    response = client.post("/producer/produce",
                           params={"topic": "test1", "producer_id": producer_id, "message": "hello"})
    assert response.status_code == 403
    assert response.json() == {"detail": "Producer not registered for this topic"}


def test_size():
    clear_db()
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    for message in range(10):
        client.post("/producer/produce",
                    params={"topic": "test", "producer_id": producer_id, "message": str(message)})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    response = client.get("/size", params={"topic": "test", "consumer_id": consumer_id})
    assert response.status_code == 200
    assert response.json() == {"size": 10}


def test_consume():
    clear_db()
    client.post("/topics", params={"name": "test"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    client.post("/producer/produce",
                params={"topic": "test", "producer_id": producer_id, "message": "hello"})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    response = client.get("/consumer/consume",
                          params={"topic": "test", "consumer_id": consumer_id})
    assert response.status_code == 200
    assert response.json() == {"message": "hello"}


def test_consume_non_existent_consumer_id():
    clear_db()
    client.post("/topics", params={"name": "test"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    client.post("/producer/produce",
                params={"topic": "test", "producer_id": producer_id, "message": "hello"})
    response = client.get("/consumer/consume",
                          params={"topic": "test", "consumer_id": "invalid"})
    assert response.status_code == 404
    assert response.json() == {"detail": "Consumer not found"}


def test_consume_non_existent_topic():
    clear_db()
    client.post("/topics", params={"name": "test"})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    response = client.get("/consumer/consume",
                          params={"topic": "test1", "consumer_id": consumer_id})
    assert response.status_code == 404
    assert response.json() == {"detail": "Topic not found"}


def test_consume_wrong_topic():
    clear_db()
    client.post("/topics", params={"name": "test"})
    client.post("/topics", params={"name": "test1"})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    response = client.get("/consumer/consume",
                          params={"topic": "test1", "consumer_id": consumer_id})
    assert response.status_code == 403
    assert response.json() == {"detail": "Consumer not registered for this topic"}


def test_consume_empty_queue():
    clear_db()
    client.post("/topics", params={"name": "test"})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    response = client.get("/consumer/consume",
                          params={"topic": "test", "consumer_id": consumer_id})
    assert response.status_code == 404
    assert response.json() == {"detail": "Queue is empty"}


def test_consume_multiple_messages():
    clear_db()
    client.post("/topics", params={"name": "test"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    for message in range(10):
        # print('posting message', message)
        client.post("/producer/produce",
                    params={"topic": "test", "producer_id": producer_id, "message": str(message)})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    for message in range(10):
        response = client.get("/consumer/consume",
                              params={"topic": "test", "consumer_id": consumer_id})
        assert response.status_code == 200
        # print("message", message)
        assert response.json() == {"message": str(message)}


def test_consume_multiple_consumers_multiple_producers():
    clear_db()
    client.post("/topics", params={"name": "test"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    producer_id_1 = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]

    for message in range(10):
        client.post("/producer/produce",
                    params={"topic": "test", "producer_id": producer_id, "message": str(message)})
        client.post("/producer/produce",
                    params={"topic": "test", "producer_id": producer_id_1, "message": str(message)})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    for message in range(10):
        response = client.get("/consumer/consume",
                              params={"topic": "test", "consumer_id": consumer_id})
        assert response.status_code == 200
        assert response.json() == {"message": str(message)}
        response = client.get("/consumer/consume",
                              params={"topic": "test", "consumer_id": consumer_id})
        assert response.status_code == 200
        assert response.json() == {"message": str(message)}


def test_consume_multiple_consumers_multiple_topics():
    clear_db()
    client.post("/topics", params={"name": "test"})
    client.post("/topics", params={"name": "test1"})
    producer_id = client.post("/producer/register", params={"topic": "test"}).json()["producer_id"]
    producer_id_1 = client.post("/producer/register", params={"topic": "test1"}).json()["producer_id"]

    for message in range(10):
        client.post("/producer/produce",
                    params={"topic": "test", "producer_id": producer_id, "message": str(message)})
        client.post("/producer/produce",
                    params={"topic": "test1", "producer_id": producer_id_1, "message": str(message)})
    consumer_id = client.post("/consumer/register", params={"topic": "test"}).json()["consumer_id"]
    for message in range(10):
        response = client.get("/consumer/consume",
                              params={"topic": "test", "consumer_id": consumer_id})
        assert response.status_code == 200
        assert response.json() == {"message": str(message)}
