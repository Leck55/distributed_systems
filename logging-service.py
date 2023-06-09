from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

messages = {}

class LogMessage(BaseModel):
    id: str
    msg: str

@app.post("/logging")
def log_request(log_message: Optional[LogMessage] = None):
    if log_message is not None and (log_message.id is not None or log_message.msg is not None):
        messages[log_message.id] = log_message.msg
        print("Received message:", log_message.msg)
        return "Success"
    else:
        raise HTTPException(status_code=400, detail="Message or ID not provided")

@app.get("/logging")
def retrieve_request():
    values = [messages[key] for key in messages]
    return values

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=25001)