from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests
import uuid

app = FastAPI()

logging_service = "http://localhost:25001/logging"
messages_service = "http://localhost:25002/messages"

class Message(BaseModel):
    id: Optional[str] = None
    msg: str

@app.post("/")
def handle_post(message: Optional[Message] = None):
    if message is not None and message.msg is not None:
        message.id = str(uuid.uuid4())
        response = requests.post(logging_service, json=message.dict())
        return {"id": message.id, "msg": message.msg}
    else:
        raise HTTPException(status_code=400, detail="Message not provided")

@app.get("/")
def handle_get():
    log_response = requests.get(logging_service)
    msg_response = requests.get(messages_service)
    return [log_response.text, msg_response.text]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=25000)