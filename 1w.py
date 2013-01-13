from random import randint
import time
import zmq
import sys
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream
from base import *

def worker_socket(context):
    worker = context.socket(zmq.DEALER)
    identity = "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
    worker.setsockopt_string(zmq.IDENTITY, identity)
    worker.connect(url_str(WORKER_URL))
    worker.send(PPP_READY)
    return worker

class ZQueue(object):
    def __init__(self, frontend_socket):
        self.frontend = ZMQStream(frontend_socket)
        self.frontend.on_recv(self.handle_frontend)

        self.liveness = HEARTBEAT_LIVENESS
        self.heartbeat = HEARTBEAT_INTERVAL
        self.interval = INTERVAL_INIT
        self.loop = IOLoop.instance()

        self.time = self.interval * self.heartbeat
        self.heartbeat_at = time.time() + self.heartbeat * HEARTBEAT_LIVENESS        
        self.callback = None
        self.timed_out = False

        self.loop.add_timeout(time.time()+self.heartbeat, self.send_heartbeat)

    def send_heartbeat(self):
        if time.time() > self.heartbeat_at:
            self.time *= 2 if self.time < INTERVAL_MAX else 1
            times_str('Timed out.. Retrying in {} seconds..'.format(self.time))
            self.callback = self.loop.add_timeout(time.time()+self.time*1, self.send_heartbeat)
            self.timed_out = True
            return
        
        times_str('sending heartbeat..')
        self.frontend.send(PPP_HEARTBEAT)
        self.loop.add_timeout(time.time()+self.heartbeat, self.send_heartbeat)

    def handle_frontend(self,msg):
        m = msg[:]
        if len(m) == 1:
            times_str('Received heartbeat')
            if self.timed_out:
                self.loop.add_timeout(time.time()+self.heartbeat, self.send_heartbeat)
                self.timed_out = False
                self.loop.remove_timeout(self.callback)
                self.time = self.interval * self.heartbeat
        elif len(m) == 3:
            times_str('Received: '+str(m))
            time.sleep(10)
            times_str('Sending it back..')
            self.frontend.send_multipart(m)
        self.heartbeat_at = time.time() + self.heartbeat * HEARTBEAT_LIVENESS

def main():
    context = zmq.Context(1)

    worker = worker_socket(context)
    queue = ZQueue(worker)

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        times_str('ctrlc')

if __name__ == "__main__":
    main()