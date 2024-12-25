#! /usr/bin/python

import json, time
import multiprocessing as mp
import socket as sk
import threading as tr
from collections import defaultdict, namedtuple
from heapq import heappop, heappush

Event = namedtuple("Event", ["clock", "pid"])
PORTS = [6000, 6005, 6010, 6015]

class ProcessHandler(mp.Process):
    def __init__(obj, id, **kwargs):
        super().__init__(**kwargs)
        obj.id = id
        obj.clock = mp.Value("i", 0)
        obj.socket = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)

    def run(obj):
        commTh = ThreadHandler(id=obj.id, clock=obj.clock)
        commTh.start()
        time.sleep(1)

        for _ in range(4):
            obj.perform()
            obj.castEvent()

    def perform(obj):
        print("~~~~~~~~~ doing some operation")

    def castEvent(obj):
        with obj.clock.get_lock():
            obj.clock.value += 1
        msg = dict(type=0, pid=obj.id, clock=obj.clock.value)
        for port in PORTS:
            obj.socket.sendto(json.dumps(msg).encode(), ("localhost", port))
        print(f"~ Event Sent P{obj.id}.{msg['clock']}")

        
class ThreadHandler(tr.Thread):
    def __init__(obj, id, clock, **kwargs):
        super().__init__(**kwargs)
        obj.id = id
        obj.clock = clock
        obj.acks = defaultdict(int)
        obj.isAcknowledged = defaultdict(bool)
        obj.socket = sk.socket(sk.AF_INET, sk.SOCK_DGRAM)
        obj.socket.bind(("", PORTS[id]))
        obj.queue = []

    def run(obj):
        while True:
            data = obj.socket.recvfrom(1024)[0]
            msg = json.loads(data.decode())
            event = Event(clock=msg["clock"], pid=msg["pid"])

            if msg["type"] == 0:
                with obj.clock.get_lock():
                    obj.clock.value += 1
                heappush(obj.queue, event)
                obj.acks[event] = 0
                if not obj.isAcknowledged[event]:
                    obj.acknowledge(event)
                    
            elif msg["type"] == 1:
                obj.acks[event] += 1
                if obj.queue:
                    if obj.acks[obj.queue[0]] >= 4:
                        obj.processEvent(heappop(obj.queue))
                    elif not obj.isAcknowledged[obj.queue[0]]:
                        obj.acknowledge(event)

    def processEvent(obj, event):
        print(f"> P{obj.id} Processed")

    def acknowledge(obj, event):
        with obj.clock.get_lock():
            obj.clock.value += 1
        data = json.dumps(dict(type=1, pid=event.pid, clock=event.clock)).encode()
        for port in PORTS:
            obj.socket.sendto(data, ("localhost", port))
        obj.isAcknowledged[event] = True

    
if __name__ == "__main__":
    processes = [ProcessHandler(id=i) for i in range(4)]

    for process in processes:
        process.start()
        time.sleep(0.01)

    for process in processes:
        process.join()
