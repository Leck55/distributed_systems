import hazelcast, consul
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import argparse
import subprocess, signal, sys, uuid, os
from typing import Optional

app = FastAPI()

c = consul.Consul()

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

def start_hazelcast_node():
    config = get_hazelcast_config()
    return subprocess.Popen(['hz', 'start', '-c', str(config)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def stop_hazelcast_node(process):
    process.terminate()
    process.wait()

def handle_interrupt(signal, frame):
    stop_hazelcast_node(hazelcast_process)
    c.agent.service.deregister("logging_service")
    sys.exit(0)

def get_hazelcast_settings():
    index, settings = c.kv.get("hazelcast_settings_map")
    if settings:
        return str(settings['Value'].decode('utf-8'))
    else:
        raise Exception("Hazelcast settings not found in Consul.")

def get_hazelcast_config():
    index, settings = c.kv.get("hazelcast_config")
    if settings:
        return str(settings['Value'].decode('utf-8'))
    else:
        raise Exception("Hazelcast configuration file not found in Consul.")


def register_service(name, port, addr):
    id = str(uuid.uuid4())
    c.agent.service.register(
        name,
        service_id=id,
        address=addr,
        port=port
    )

class LogMessage(BaseModel):
    id: str
    msg: str

@app.post("/logging")
def log_request(log_message: Optional[LogMessage] = None):
    if log_message is not None and (log_message.id is not None or log_message.msg is not None):
        messages.put(log_message.id, log_message.msg)
        print("Received message:", log_message.msg) 
        return "Success"
    else:
        raise HTTPException(status_code=400, detail="Message or ID not provided")

@app.get("/logging")
def retrieve_request():
    keys = messages.key_set()
    values = [messages.get(key) for key in keys]
    return values

if __name__ == "__main__":
    import uvicorn
    print("Current Process ID:", os.getpid())
    #signal.signal(signal.SIGINT, handle_interrupt)
    addr = os.getenv('SERVICE_IP_MESSAGES', 'localhost')
    register_service('logging_service', args.port, addr)
    hazelcast_process = start_hazelcast_node()
    print("A hazelcast node has started successfuly")
    hz = hazelcast.HazelcastClient()
    messages = hz.get_map(get_hazelcast_settings()).blocking()
    uvicorn.run(app, host=addr, port=args.port)
