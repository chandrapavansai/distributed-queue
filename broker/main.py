import time

from fastapi import FastAPI, status, Depends, HTTPException
from sqlalchemy.orm import Session

from . import crud, models, schemas
from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)
app = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.get("/messages", response_model=schemas.Message)
def get_message(topic: str, partition: int, offset: int = 0, db: Session = Depends(get_db)):
    result = crud.get_message(db, topic, partition, offset)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No message found")
    return result


@app.post("/messages", response_model=schemas.Message, status_code=status.HTTP_201_CREATED)
def post_message(message: schemas.MessageCreate, db: Session = Depends(get_db)):
    return crud.create_message(db, message)


@app.get("/messages/count")
def get_message_count(topic: str, partition: int, offset: int = 0, db: Session = Depends(get_db)):
    return crud.get_message_count(db,topic, partition, offset)
