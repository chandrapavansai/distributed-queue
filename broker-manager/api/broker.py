import hashing.hash as Hash
from database import db
from fastapi import APIRouter, HTTPException

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
                "ip": crud.get_broker_ip(broker_id, cursor)
            } for broker_id in Hash.get_active_brokers()
        ]
    }

    # No need to commit as we are not changing anything
    # db.commit()


@router.post("/")
def create_broker(ip_addr: str):
    """
    Endpoint to create a broker
    :return: success message
    """
    # Need to be a leader broker manager
    # Add the broker to the database
    cursor = db.cursor()
    new_broker_id = Hash.add_broker(ip_addr, cursor)
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
    ip_addr = crud.get_broker_ip(id, cursor)
    cur_size = Hash.remove_brokers([id,], cursor)
    db.commit()
    return {"message": "Broker deleted", "ip": ip_addr, "current_count": cur_size}
