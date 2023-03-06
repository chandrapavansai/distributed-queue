import crud
from fastapi import APIRouter, HTTPException

router = APIRouter(
    prefix="/heartbeat",
)


@router.put("/consumer")
def heartbeat_consumer(consumer_id: str):
    """
    Endpoint to send heartbeat
    :return: success message
    """
    return {"message": "alive"}


@router.put("/producer")
def heartbeat_producer(producer_id: str):
    """
    Endpoint to send heartbeat
    :return: success message
    """
    # Set the last heartbeat time in the database
    return {"message": "alive"}


@router.put("/broker")
def heartbeat_broker(broker_id: str):
    """
    Endpoint to send heartbeat
    :return: success message
    """
    # Set the last heartbeat time in the database
    return {"message": "alive"}


@router.put("/manager")
def heartbeat_manager(manager_id: str):
    """
    Endpoint to send heartbeat
    :return: success message
    """
    # Set the last heartbeat time in the database
    return {"message": "alive"}