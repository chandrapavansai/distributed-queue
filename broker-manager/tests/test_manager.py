import os
from fastapi.testclient import TestClient
from main import app
from database import clear_db
from api import crud
from uuid import uuid4

client = TestClient(app)


def test_ping():
    clear_db()
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"message": "pong"}


def test_broker_create():
    clear_db()
    response = client.post("/broker?url=http://raft-broker1:8000")
    assert response.status_code == 200
    assert response.json() == {"message": "Broker created", "new_id": 0}


def test_broker_list():
    clear_db()
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.get("/broker")
    assert response.status_code == 200
    assert response.json() == {"brokers": [
        {"id": 0, "url": "http://raft-broker1:8000"},
        {"id": 1, "url": "http://raft-broker2:8000"}
    ]
    }


def test_get_topics_empty():
    clear_db()
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {"topics": []}


def test_create_topic_fail():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post(f"/topics?name={topic_name}")
    assert response.status_code == 503


def test_create_topic():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    assert response.status_code == 201
    assert response.json() == {"message": "Topic created"}
    # See if client is created
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {'topics': [{f"{topic_name}": [0]}]}


def test_create_topic_with_partitions_fail():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post(f"/topics?name={topic_name}&partitions=2")
    assert response.status_code == 503


def test_create_topic_with_partitions_fail():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}&partitions=2")
    assert response.status_code == 201
    assert response.json() == {"message": "Topic created"}
    # See if topic is created
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {'topics': [{f"{topic_name}": [0, 1]}]}


def test_create_topic_with_partitions_check_broker():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}&partitions=2")
    assert response.status_code == 201
    assert response.json() == {"message": "Topic created"}
    # See if topic is created
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {'topics': [{f"{topic_name}": [0, 1]}]}

    # !! Don't use this function
    # See if broker is assigned
    assert sorted(crud.get_brokers_id_from_topic(topic_name, 0)) == [0, 1, 2]
    assert sorted(crud.get_brokers_id_from_topic(topic_name, 1)) == [0, 1, 2]


def test_create_partition():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}&partitions=2")
    assert response.status_code == 201
    assert response.json() == {"message": "Topic created"}
    # See if topic is created
    response = client.get("/topics")
    assert response.status_code == 200
    assert response.json() == {'topics': [{f"{topic_name}": [0, 1]}]}
    response = client.post(f"/topics/partitions?topic={topic_name}")
    assert response.status_code == 201
    assert response.json() == {"message": "Partition created", "partition": 2}


def test_consumer_register_check_topic_not_exists():
    clear_db()
    topic_name = str(uuid4())
    response = client.post(f"/consumer/register?topic={topic_name}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Topic does not exist"}


def test_consumer_register_check_partition_not_exists():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    response = client.post(
        f"/consumer/register?topic={topic_name}&partition=1")
    assert response.status_code == 404
    assert response.json() == {"detail": "Partition does not exist"}


def test_consumer_register_works():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    response = client.post(f"/consumer/register?topic={topic_name}")
    assert response.status_code == 200
    assert len(response.json()) == 36


def test_consume_check_consumer_not_exists():
    clear_db()
    topic_name = str(uuid4())
    response = client.get(
        f"/consumer/consume?consumer_id=123&topic={topic_name}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Consumer does not exist"}


def test_consume_check_topic_not_registered():
    clear_db()
    topic_name = str(uuid4())
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    response = client.post(f"/consumer/register?topic={topic_name}")
    consumer_id = response.json()
    response = client.get(
        f"/consumer/consume?consumer_id={consumer_id}&topic=random_topic")
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Consumer is not registered to this topic"}


def test_consume_check_partition_not_registered():
    clear_db()
    topic_name = str(uuid4())
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}&partitions=2")
    response = client.post(
        f"/consumer/register?topic={topic_name}&partition=0")
    consumer_id = response.json()
    response = client.get(
        f"/consumer/consume?consumer_id={consumer_id}&topic={topic_name}&partition=1")
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Consumer is not registered to this partition"}


def test_producer_register_check_topic_not_exists():
    clear_db()
    topic_name = str(uuid4())
    response = client.post(f"/producer/register?topic={topic_name}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Topic does not exist"}


def test_producer_register_check_partition_not_exists():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    response = client.post(
        f"/producer/register?topic={topic_name}&partition=1")
    assert response.status_code == 404
    assert response.json() == {"detail": "Partition does not exist"}


def test_producer_register_works():
    clear_db()
    topic_name = str(uuid4())
    # Create a broker
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    response = client.post(f"/producer/register?topic={topic_name}")
    assert response.status_code == 200
    assert len(response.json()) == 36


def test_produce_producer_not_exists():
    response = client.post(
        "/producer/produce?producer_id=123&topic={topic_name}&message=hello")
    assert response.status_code == 404
    assert response.json() == {"detail": "Producer does not exist"}


def test_produce_topic_not_registered():
    clear_db()
    topic_name = str(uuid4())
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}")
    response = client.post(f"/producer/register?topic={topic_name}")
    producer_id = response.json()
    response = client.post(
        f"/producer/produce?producer_id={producer_id}&topic=random_topic&message=hello")
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Producer is not registered to this topic"}


def test_produce_partition_not_registered():
    clear_db()
    topic_name = str(uuid4())
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    response = client.post(f"/topics?name={topic_name}&partitions=2")
    response = client.post(
        f"/producer/register?topic={topic_name}&partition=0")
    producer_id = response.json()
    response = client.post(
        f"/producer/produce?producer_id={producer_id}&topic={topic_name}&partition=1&message=hello")
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Producer is not registered to this partition"}


# def test_list_managers():
#     clear_db()
#     # Set environment variable
#     os.environ["MGR_LEADER_URL"] = "leader.com"
#     os.environ["MGR_URL"] = "leader.com"

#     with TestClient(app) as client:
#         response = client.get("/managers")
#         assert response.status_code == 200
#         assert response.json() == {'managers': [['localhost', True]]}

def test_broker_remove_check_broker_not_exists():
    clear_db()
    response = client.delete("/broker?id=123")
    assert response.status_code == 404
    assert response.json() == {"detail": "Broker not found"}


def test_broker_remove_works():
    clear_db()
    response = client.post("/broker?url=http://raft-broker1:8000")
    response = client.post("/broker?url=http://raft-broker2:8000")
    response = client.post("/broker?url=http://raft-broker3:8000")
    broker_id = response.json()["new_id"]
    response = client.delete(f"/broker?id={broker_id}")
    assert response.status_code == 200
    assert response.json() == {"message": "Broker deleted",
                               "url": "http://raft-broker3:8000", "current_count": 2}

