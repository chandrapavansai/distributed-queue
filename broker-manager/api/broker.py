import hashing
from database import db
from fastapi import APIRouter

from . import crud

router = APIRouter(
    prefix="/broker",
)


@router.get("/")
def list_brokers():
    """
    Endpoint to list all brokers
    :return: list of brokers
    """
    cursor = db.cursor()
    # Get the topics and partitions using dictionary comprehension
    return {
        "brokers": [
            {
                "id": broker_id,
                "url": crud.get_broker_url(broker_id, cursor)
            } for broker_id in hashing.get_active_brokers()
        ]
    }

    # No need to commit as we are not changing anything
    # db.commit()


@router.post("/")
def create_broker(url: str):
    """
    Endpoint to create a broker
    :return: success message
    """
    # Need to be a leader broker manager
    # Add the broker to the database
    cursor = db.cursor()
    new_broker_id = hashing.add_broker(url, cursor)
    db.commit()
    return {"message": "Broker created", "new_id": new_broker_id}


@router.delete("/")
def delete_broker(id: int):
    """
    Endpoint to create a broker
    :return: success message
    """
    # Need to be a leader broker manager
    # Add the broker to the database
    cursor = db.cursor()
    url = crud.get_broker_url(id, cursor)
    cur_size = hashing.remove_brokers([id, ], cursor)
    db.commit()
    return {"message": "Broker deleted", "url": url, "current_count": cur_size}
