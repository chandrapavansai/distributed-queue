from fastapi import APIRouter, HTTPException
import crud

router = APIRouter(
    prefix="/managers",
)


@router.get("/")
def get_managers():

    cursor = db.cursor()
    return {
        "managers": crud.get_alive_managers(cursor)
    }

    # No need to commit as we are not changing anything
    # db.commit()
