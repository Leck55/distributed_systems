import hazelcast
from threading import Thread
import time


def startThreads(function):
    threads = []
    for i in range(3):
        process = Thread(target=function, args=[])
        process.start()
        threads.append(process)
    for thread in threads:
        thread.join()

def unprotected():
    hz = hazelcast.HazelcastClient()
    map1 = hz.get_map('wl').blocking()
    for i in range(1000):
        counter = map1.get('wl')
        counter += 1
        map1.put(i, f"value-{i}")
    
def pessimistic():
    hz = hazelcast.HazelcastClient()
    map2 = hz.get_map('pl').blocking()
    for i in range(1000):
        map2.lock('pl')
        try:
            counter = map2.get('pl')
            counter += 1
            map2.put('pl', counter)
        finally:
            map2.unlock('pl')


def optimistic():
    hz = hazelcast.HazelcastClient()
    map3 = hz.get_map('ol').blocking()
    for i in range(1000):
        while True:
            og = map3.get('ol')
            new = og
            new += 1
            if map3.replace_if_same('ol',og,new):
                break


hz = hazelcast.HazelcastClient()
map1 = hz.get_map('wl').blocking()
map1.set('wl', 0)
map2 = hz.get_map('pl').blocking()
map2.set('pl', 0)
map3 = hz.get_map('ol').blocking()
map3.set('ol', 0)

start_time = time.time()
print('Commensing execution of an race condition case without locking')
startThreads(unprotected)
print("Completion time is %s seconds" % (time.time() - start_time))
print('Counter value: %s' % map1.get('wl'))

start_time = time.time()
print('\nCommensing execution of a race condition case with pessimistic locking')
startThreads(pessimistic)
print("Completion time is %s seconds" % (time.time() - start_time))
print('Counter value: %s' % map2.get('pl'))

start_time = time.time()
print('\nCommensing execution of a race condition case with optimistic locking')
startThreads(optimistic)
print("Completion time is %s seconds" % (time.time() - start_time))
print('Counter value: %s' % map3.get('ol'))


