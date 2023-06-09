import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import argparse
import subprocess, signal, sys
from typing import Optional

app = FastAPI()

port_rng = [25001, 25002, 25003]

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

def start_hazelcast_node():
    return subprocess.Popen(['hz', 'start'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def stop_hazelcast_node(process):
    process.terminate()
    process.wait()

def handle_interrupt(signal, frame):
    stop_hazelcast_node(hazelcast_process)
    sys.exit(0)


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
    if args.port not in port_rng:
        print('Invalid port. Allowed ports: 25001, 25002, 25003')
        sys.exit(1)
    import uvicorn
    hazelcast_process = start_hazelcast_node()
    print("A hazelcast node has started successfuly")
    hz = hazelcast.HazelcastClient()
    messages = hz.get_map("messages").blocking()
    uvicorn.run(app, host="0.0.0.0", port=args.port)
    signal.signal(signal.SIGINT, handle_interrupt)
