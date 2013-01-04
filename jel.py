from random import randint
import helpers
import time
import zmq
import sys
from settings import *
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

class ZQueue(object):
    def __init__(self, frontend_socket, backend_socket):
        self.queue = helpers.WorkerQueue()
        self.frontend = ZMQStream(frontend_socket)
        self.backend = ZMQStream(backend_socket)
        
        self.liveness = HEARTBEAT_LIVENESS
        self.heartbeat = HEARTBEAT_INTERVAL
        self.interval = INTERVAL_INIT
        self.loop = IOLoop.instance()
        self.hearbeats = 0

        self.frontend.on_recv(self.handle_frontend)
        self.backend.on_recv(self.handle_backend)
        self.period = PeriodicCallback(self.purge,HEARTBEAT_INTERVAL*1000)
        self.period.start()

    def handle_frontend(self,msg):
        m = msg[:]
        if len(m) == 1:
            helpers.Helpers.times_str('Received heartbeat')
            if self.timed_out:
                self.loop.add_timeout(time.time()+HEARTBEAT_INTERVAL, self.send_heartbeat)
                self.timed_out = False
                self.loop.remove_timeout(self.callback)
        elif len(m) == 3:
            helpers.Helpers.times_str('Received: '+str(m))
            address, worker = self.queue.getLRU()
            worker.working = True
            m.insert(0,address)
            self.backend.send_multipart(m)
        self.heartbeat_at = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    def handle_backend(self,msg):
        m = msg[:]
        address = m[0]

        helpers.Helpers.times_str('Backend Received: {}'.format(m))
        self.queue.ready(helpers.Worker(address))
        self.backend.send_multipart([address,PPP_HEARTBEAT])

        mm = m[1:]
        if len(mm) == 1:
            if mm[0] == PPP_HEARTBEAT:
                self.hearbeats += 1
                helpers.Helpers.times_str('Got hearbeat {}'.format(self.hearbeats))               
        else:
            helpers.Helpers.times_str('Sending it back..')
            self.frontend.send_multipart(mm)

        if not self.queue.empty():
            self.frontend.on_recv(self.handle_frontend)

    def purge(self):
        self.queue.purge()
        if self.queue.empty():
            self.frontend.stop_on_recv()

    def send_heartbeat(self):
        if time.time() > self.heartbeat_at:
            self.time *= 2 if self.time < INTERVAL_MAX else 1
            helpers.Helpers.times_str('Timed out.. Retrying in {} seconds..'.format(self.time))
            self.callback = self.loop.add_timeout(time.time()+self.time*1, self.send_heartbeat)
            self.timed_out = True
            return
        self.time = self.interval * self.heartbeat
        helpers.Helpers.times_str('sending heartbeat..')
        self.frontend.send(PPP_HEARTBEAT)
        self.loop.add_timeout(time.time()+self.heartbeat, self.send_heartbeat)

    def run(self):
        try:
            self.loop.start()
        except KeyboardInterrupt:
            helpers.Helpers.times_str('ctrlc')

class Runner(object):
    def __init__(self):
        context = zmq.Context(1)
        self.frontend = context.socket(zmq.DEALER)
        self.backend = context.socket(zmq.ROUTER)
    def start(self,front,back):
        self.frontend.connect(front)
        self.backend.bind(back)
        self.runner = ZQueue(self.frontend,self.backend)
        self.runner.run()
def main():
    r = Runner();
    r.start('tcp://*:5555','tcp://*:5556');

if __name__ == "__main__":
    main()