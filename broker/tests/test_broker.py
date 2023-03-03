import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ..database import Base
from ..main import app, get_db

SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def test_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)


def test_ping(test_db):
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"message": "pong"}


def test_post_message(test_db):
    content = "Testing post message"
    response = client.post("/messages", json={"topic": "test", "partition": 1, "content": content})
    assert response.status_code == 201
    assert response.json()["content"] == content


def test_messages_count_initial(test_db):
    response = client.get("/messages/count?topic=test&partition=1")
    assert response.status_code == 200
    assert response.json() == 0


def test_messages_count_after_posting(test_db):
    for i in range(10):
        client.post("/messages", json={"topic": "test", "partition": 1, "content": f"Testing {i}"})
        response = client.get("/messages/count", params={"topic": "test", "partition": 1})
        assert response.status_code == 200
        assert response.json() == i + 1


def test_get_message_empty_queue(test_db):
    response = client.get("/messages", params={"topic": "test", "partition": 1})
    assert response.status_code == 404
    assert response.json() == {"detail": "No message found"}


def test_get_message_higer_offset(test_db):
    response = client.get("/messages", params={"topic": "test", "partition": 1, "offset": 10})
    assert response.status_code == 404
    assert response.json() == {"detail": "No message found"}


def test_get_message(test_db):
    message = {
        "topic": "",
        "partition": 1,
        "content": "testing get message"
    }
    client.post("/messages", json=message)
    response = client.get("/messages", params={"topic": message["topic"], "partition": message["partition"]})
    assert response.status_code == 200
    assert response.json()["content"] == message["content"]


def test_get_message_with_offset(test_db):
    def message(i):
        return {
            "topic": "test",
            "partition": 1,
            "content": f"testing get message {i}"
        }

    for i in range(10):
        client.post("/messages", json=message(i))

    for i in range(10):
        response = client.get(
            "/messages", params={"topic": message(i)["topic"], "partition": message(i)["partition"], "offset": i}
        )
        assert response.status_code == 200
        assert response.json()["content"] == message(i)["content"]


def test_get_message_count_with_offset(test_db):
    def message(i):
        return {
            "topic": "test",
            "partition": 1,
            "content": f"testing get message {i}"
        }

    for i in range(10):
        client.post("/messages", json=message(i))

    for i in range(10):
        response = client.get(
            "/messages/count", params={"topic": message(i)["topic"], "partition": message(i)["partition"], "offset": i}
        )
        assert response.status_code == 200
        assert response.json() == 10 - i
