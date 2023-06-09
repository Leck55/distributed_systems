import hazelcast

produ = hazelcast.HazelcastClient(
    cluster_name="dev",
    cluster_members=[
        "127.0.0.1:5701"
    ],
)

queue_name = "queue"

queue = produ.get_queue(queue_name).blocking()
for i in range(100):
    queue.put(i)
    print("Producing message #" + str(i))
queue.put(-1)
print("Producing finished")

