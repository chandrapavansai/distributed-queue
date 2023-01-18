from fastapi import FastAPI

app = FastAPI()


@app.get("/ping")
async def ping():
    return {"message": "pong"}


# @app.get("/hello/{name}")
# async def say_hello(name: str):
#     return {"message": f"Hello {name}"}

@app.post("/topics")
async def create_topic():
    ...


@app.get("/topics")
async def list_topics():
    ...


@app.post("/consumer/register")
async def register_consumer():
    ...


@app.post("/producer/register")
async def register_producer():
    ...


@app.post("/producer/produce")
async def enqueue():
    ...


@app.get("/consumer/consume")
async def dequeue():
    ...


@app.get("/size")
async def size():
    ...


