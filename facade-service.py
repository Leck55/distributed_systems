from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests, json
import uuid
import random
import hazelcast

app = FastAPI()


class Message(BaseModel):
    id: Optional[str] = None
    msg: str

@app.post("/")
def handle_post(message: Optional[Message] = None):
    if message is not None:
        if message.msg is None:
            raise HTTPException(status_code=400, detail="Message not provided")
        message.id = str(uuid.uuid4())
        port_log = random.randint(25001, 25003)
        logging_service = "http://localhost:{}/logging".format(port_log)
        response = requests.post(logging_service, json=message.dict())
        queue.put(json.dumps(message.msg))
        return {"id": message.id, "msg": message.msg}

@app.get("/")
def handle_get():
    port_log = random.randint(25001, 25003)
    log_response = requests.get(f"http://localhost:{port_log}/logging")
    port_msg = random.randint(25004, 25005)
    msg_response = requests.get(f"http://localhost:{port_msg}/messages")
    return [log_response.text, msg_response.text]

if __name__ == "__main__":
    import uvicorn
    hz = hazelcast.HazelcastClient()
    queue = hz.get_queue("message-queue").blocking()
    uvicorn.run(app, host="0.0.0.0", port=25000)
