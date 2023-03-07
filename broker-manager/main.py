from fastapi import FastAPI

from api import consumer, producer, topics, heartbeat, managers, broker
import utils

app = FastAPI(title="Distributed Message Queue", version="0.2.0")

utils.claim_existence()

if utils.is_leader:
    # TODO:
    # Run the Heartbeat Algorithm in a separate thread

    app.include_router(managers.router)
    app.include_router(broker.router)
    app.include_router(topics.router)
    app.include_router(producer.router)
    app.include_router(heartbeat.router)
else:
    app.include_router(topics.router)
    app.include_router(consumer.router)


@app.get("/ping")
def ping():
    return {"message": "pong"}
