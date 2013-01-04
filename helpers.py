import time
import sys
from settings import *
from collections import OrderedDict

class Helpers(object):
	def times():
		return time.strftime('%H:%M:%S')
	def times_str(s):
		print('[{}] {}'.format(Helpers.times(),str(s)))

class Worker(object):
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
            Helpers.times_str('W: Idle worker expired: {}'.format(address))
            self._queue.pop(address, None)

    def empty(self):
        if len(self._queue) == 0:
            return True
        return False

    def ready(self, worker):
        self._queue.pop(worker.address, None)
        self._queue[worker.address] = worker

    def getLRU(self):
        return self._queue.popitem(False)