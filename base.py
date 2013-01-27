import time
from collections import OrderedDict

HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 1
INTERVAL_INIT = 1
INTERVAL_MAX = 32
PPP_READY = b'\x01'      
PPP_HEARTBEAT = b'\x02'

WORKER_URL = ('tcp','localhost','7000')
CLIENT_URL = ('tcp','theone','5555')

def times():
    return time.strftime('%H:%M:%S')

def times_str(s):
    print('[{}] {}'.format(times(),str(s)))

def url_str(t,bind=False):
    if bind:
        return '{}://*:{}'.format(t[0],t[2])
    return '{}://{}:{}'.format(*t)

class WorkerModel(object):
    def __init__(self, address):
        self.address = address
        self.working = False
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class WorkerQueue(object):
    def __init__(self):
        self._queue = OrderedDict()

    def purge(self):
        expired = []
        for address,worker in self._queue.items():
            if ( (time.time() > worker.expiry) and not (worker.working) ): 
                expired.append(address)
        for address in expired:
            times_str('Q: Idle worker expired: {}'.format(address))
            self._queue.pop(address, None)

    def empty(self):
        return True if len(self._queue) == 0 else False

    def ready(self, worker):
        self._queue.pop(worker.address, None)
        self._queue[worker.address] = worker

    def getLRU(self):
        return self._queue.popitem(False)