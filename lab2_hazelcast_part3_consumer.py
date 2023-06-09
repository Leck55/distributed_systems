import hazelcast

consu = hazelcast.HazelcastClient(
    cluster_name="dev",
    cluster_members=[
        "127.0.0.1:5702",
        "127.0.0.1:5703"
    ],
)

queue_name = "queue"
queue = consu.get_queue(queue_name).blocking()

while True:
    item = queue.take()
    print("Consumed " + str(item))
    if (item == -1):
        queue.put(-1)
        break
