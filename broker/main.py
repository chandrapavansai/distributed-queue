from fastapi import FastAPI
import time
from database import db
app = FastAPI()

cursor = db.cursor()
# Print PostgreSQL details
print("PostgreSQL server information")
print(db.get_dsn_parameters(), "\n")




@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
    return response




@app.get("/ping")
async def ping():
    return {"message": "pong"}


# @app.get("/hello/{name}")
# async def say_hello(name: str):
#     return {"message": f"Hello {name}"}

@app.post("/topics/{name}")
async def create_topic(name: str):
    ...


@app.get("/topics")
async def list_topics():
    ...


@app.post("/consumer/register/{topic}")
async def register_consumer(topic: str):
    ...


@app.post("/producer/register/{topic}")
async def register_producer(topic: str):
    ...


@app.post("/producer/produce/{topic}")
async def enqueue(topic: str):
    ...


@app.get("/consumer/consume/{topic}")
async def dequeue(topic: str):
    ...


@app.get("/size/{topic}")
async def size(topic: str):
    ...
