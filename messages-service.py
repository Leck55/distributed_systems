from fastapi import FastAPI
from hazelcast import HazelcastClient
import json
import argparse, sys, threading

app = FastAPI()

port_rng = [25004, 25005]

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()

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
    if args.port not in port_rng:
        print('Invalid port. Allowed ports: 25004, 25005')
        sys.exit(1)
    import uvicorn
    hz = HazelcastClient()
    queue = hz.get_queue("message-queue").blocking()
    thread = threading.Thread(target=consume_queue, args=(queue,))
    thread.start()
    uvicorn.run(app, host="0.0.0.0", port=args.port)