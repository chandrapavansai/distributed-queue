import os

from api import crud
from database import db

ip_addr = os.environ.get("MGR_IP")
is_leader = os.environ.get("MGR_LEADER") == ip_addr

def claim_existence():
    """
    Utility function to claim existence of the manager
    """
    cursor = db.cursor()
    print("Creating manager", ip_addr, is_leader)
    crud.create_manager(ip_addr, is_leader, cursor)
    db.commit()
