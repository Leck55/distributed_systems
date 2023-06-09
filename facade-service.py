from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests
import uuid
import random

app = FastAPI()

messages_service = "http://localhost:25004/messages"

class Message(BaseModel):
    id: Optional[str] = None
    msg: str

@app.post("/")
def handle_post(message: Optional[Message] = None):
    if message is not None and message.msg is not None:
        message.id = str(uuid.uuid4())
        port = random.randint(25001, 25003)
        logging_service = "http://localhost:{}/logging".format(port)
        response = requests.post(logging_service, json=message.dict())
        return {"id": message.id, "msg": message.msg}
    else:
        raise HTTPException(status_code=400, detail="Message not provided")
    
@app.get("/")
def handle_get():
    port = random.randint(25001, 25003)
    log_response = requests.get("http://localhost:{}/logging".format(port))
    msg_response = requests.get(messages_service)
    return str([log_response.text, msg_response.text])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=25000)
