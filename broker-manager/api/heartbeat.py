from . import crud
from fastapi import APIRouter, HTTPException
from database import db

router = APIRouter(
    prefix="/heartbeat",
)


@router.post("/consumer")
def heartbeat_consumer(consumer_id: str):
    """
    Endpoint to send heartbeat
    :return: success message
    """
    cursor = db.cursor()
    crud.set_producer_heartbeat(consumer_id, cursor)
    return {"message": "alive"}


@router.post("/producer")
def heartbeat_producer(producer_id: str):
    """
    Endpoint to send heartbeat
    :return: success message
    """
    # Set the last heartbeat time in the database
    cursor = db.cursor()
    crud.set_producer_heartbeat(producer_id, cursor)
    return {"message": "alive"}

# Broker and Manager heartbeats are directly checked in the heartbeat thread
