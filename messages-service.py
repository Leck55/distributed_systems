from fastapi import FastAPI
from hazelcast import HazelcastClient
import json, consul
import argparse, sys, threading, uuid, os, signal

app = FastAPI()

c = consul.Consul()

def handle_interrupt(signal, frame):
    c.agent.service.deregister("facade_service")
    sys.exit(0)

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

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

messages = []

@app.get('/messages')
def get_messages():
    return messages

def consume_queue(queue):
    while True:
        queued_message = queue.take()
        if queued_message:
            print('Received message: '+queued_message)
            messages.append(queued_message)

if __name__ == "__main__":
    import uvicorn
    signal.signal(signal.SIGINT, handle_interrupt)
    addr = os.getenv('SERVICE_IP_MESSAGES', 'localhost')
    register_service('messages_service', args.port, addr)
    hz = HazelcastClient()
    queue = hz.get_queue(get_hazelcast_settings()).blocking()
    thread = threading.Thread(target=consume_queue, args=(queue,))
    thread.start()
    uvicorn.run(app, host=addr, port=args.port)