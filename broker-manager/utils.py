import os

from api import crud
from database import db

url = os.environ.get("MGR_URL")
is_leader = os.environ.get("MGR_LEADER_URL") == url


def claim_existence():
    """
    Utility function to claim existence of the manager
    """
    cursor = db.cursor()
    print("Creating manager", url, is_leader)
    crud.create_manager(url, is_leader, cursor)
    db.commit()
