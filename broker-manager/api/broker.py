from fastapi import APIRouter, HTTPException

from . import crud
from database import db

router = APIRouter(
    prefix="/broker",
)

@router.post("/")
def create_broker(ip_addr: str):
    """
    Endpoint to create a broker
    :return: success message
    """
    # Need to be a leader broker manager
    # Add the broker to the database
    cursor = db.cursor()
    crud.create_broker(0, ip_addr, cursor)
    db.commit()
    return {"message": "Broker created"}