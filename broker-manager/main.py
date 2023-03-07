from fastapi import FastAPI

from api import consumer, producer, topics, heartbeat, managers, broker
import utils

app = FastAPI(title="Distributed Message Queue", version="0.2.0")

@app.on_event("startup")
async def startup_event():
    utils.claim_existence()
    
# Create a thread to run the heartbeat algorithm


if utils.is_leader:
    # TODO:
    # Run the Heartbeat Algorithm in a separate thread
    # utils.HeartbeatThread().start()

    app.include_router(managers.router)
    app.include_router(broker.router)
    app.include_router(topics.router)
    app.include_router(producer.router)
    app.include_router(heartbeat.router)
    app.include_router(topics.router)
    app.include_router(consumer.router)
    
# else:


@app.get("/ping")
def ping():
    return {"message": "pong"}
