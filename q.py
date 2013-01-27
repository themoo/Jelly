from random import randint
import time
import zmq
import sys

from base import *
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

class TheQueue(object):
    def __init__(self, context, front, back, top=True):
        self.workers = WorkerQueue()
        self.loop = IOLoop.instance()
                
        self.liveness = HEARTBEAT_LIVENESS
        self.heartbeat = HEARTBEAT_INTERVAL
        self.interval = INTERVAL_INIT       
        self.time = self.interval * self.heartbeat
        self.heartbeat_at = time.time() + self.heartbeat * HEARTBEAT_LIVENESS

        self.callback = None
        self.timed_out = False
        self.hearbeats = 0

        if top:
            self.frontend_socket = context.socket(zmq.ROUTER)
            self.frontend_socket.bind(url_str(front,True))
        else:
            self.frontend_socket = context.socket(zmq.DEALER)
            self.frontend_socket.connect(url_str(front))
            self.frontend.send(PPP_READY)
        self.backend_socket = context.socket(zmq.ROUTER)
        self.backend_socket.bind(url_str(back,True))

        self.frontend = ZMQStream(self.frontend_socket)
        self.backend = ZMQStream(self.backend_socket)

        self.start()

    def start(self):
        self.frontend.on_recv(self.handle_frontend)
        self.backend.on_recv(self.handle_backend)
        self.period = PeriodicCallback(self.workers.purge, self.heartbeat*1000)
        self.period.start()

        try:
            self.frontend.send(PPP_READY)
            #self.loop.start()
        except KeyboardInterrupt:
            times_str('ctrlc1')

    def handle_frontend(self,msg):
        m = msg[:]
        if len(m) == 1:
            times_str('Q: Received Heartbeat')
            if self.timed_out:
                self.loop.add_timeout(time.time()+self.heartbeat, self.send_heartbeat)
                self.timed_out = False
                self.loop.remove_timeout(self.callback)
        elif len(m) == 3:
            times_str('Received: '+str(m))
            address, worker = self.workers.getLRU()
            worker.working = True
            m.insert(0,address)
            self.backend.send_multipart(m)
        self.heartbeat_at = time.time() + self.heartbeat * self.liveness

    def handle_backend(self,msg):
        m = msg[:]
        address = m[0]

        #times_str('Backend Received: {}'.format(m))
        self.workers.ready(WorkerModel(address))
        self.backend.send_multipart([address,PPP_HEARTBEAT])

        mm = m[1:]
        if len(mm) == 1:
            if mm[0] == PPP_HEARTBEAT:
                self.hearbeats += 1
                times_str('Got hearbeat {}'.format(self.hearbeats))               
        else:
            times_str('Sending it back..')
            self.frontend.send_multipart(mm)

    def send_heartbeat(self):
        if time.time() > self.heartbeat_at:
            self.time *= 2 if self.time < INTERVAL_MAX else 1
            times_str('Timed out.. Retrying in {} seconds..'.format(self.time))
            self.callback = self.loop.add_timeout(time.time()+self.time, self.send_heartbeat)
            self.timed_out = True
            return
        self.time = self.interval * self.heartbeat
        times_str('Q: Sending Heartbeat..')
        self.frontend.send(PPP_HEARTBEAT)
        self.loop.add_timeout(time.time()+self.heartbeat, self.send_heartbeat)

def main():
    context = zmq.Context(1)
    #q = TheQueue(context, CLIENT_URL, WORKER_URL)

if __name__ == "__main__":
    main()