from fastapi.testclient import TestClient
from main import app
from database import clear_db

client = TestClient(app)


def test_ping():
    clear_db()
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"message": "pong"}


