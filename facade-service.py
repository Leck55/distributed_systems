from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import requests, json
import uuid
import random
import hazelcast, argparse, consul, os, signal, sys

app = FastAPI()

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

def handle_interrupt(signal, frame):
    c.agent.service.deregister("facade_service")
    sys.exit(0)

c = consul.Consul()

def get_service(name):
    index, service = c.catalog.service(name)
    if service:
        return f"http://{service[0]['ServiceAddress']}:{service[0]['ServicePort']}"
    else:
        raise Exception(f"Service '{name}' not found in Consul.")

def get_hazelcast_settings():
    index, settings = c.kv.get("hazelcast_settings_queue")
    if settings:
        return str(settings['Value'].decode('utf-8'))
    else:
        raise Exception("Hazelcast settings not found in Consul.")

def register_service(name, port, addr):
    id = str(uuid.uuid4())
    c.agent.service.register(
        name,
        service_id=id,
        address=addr,
        port=port
    )

class Message(BaseModel):
    id: Optional[str] = None
    msg: str

@app.post("/")
def handle_post(message: Optional[Message] = None):
    if message is not None:
        if message.msg is None:
            raise HTTPException(status_code=400, detail="Message not provided")
        message.id = str(uuid.uuid4())
        response = requests.post(f"{log_addr}/logging", json=message.dict())
        queue.put(json.dumps(message.msg))
        return {"id": message.id, "msg": message.msg}

@app.get("/")
def handle_get():
    log_response = requests.get(f"{log_addr}/logging")
    msg_response = requests.get(f"{msg_addr}/messages")
    return [log_response.text, msg_response.text]

if __name__ == "__main__":
    import uvicorn
    signal.signal(signal.SIGINT, handle_interrupt)
    addr = os.getenv('SERVICE_IP_FACADE', 'localhost')
    register_service('facade_service', args.port, addr)
    log_addr = get_service('logging_service')
    msg_addr = get_service('messages_service')
    hz = hazelcast.HazelcastClient()
    queue = hz.get_queue(get_hazelcast_settings()).blocking()
    uvicorn.run(app, host=addr, port=args.port)
