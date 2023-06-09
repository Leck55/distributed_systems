import hazelcast

client = hazelcast.HazelcastClient()

my_map = client.get_map("my-distributed-map").blocking()

for i in range(1000):                                           # Task 1
    my_map.put(i, f"value-{i}")

print("1000 entries have been added to the distributed map.")

client.shutdown()