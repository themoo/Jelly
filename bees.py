import sys
import time
import zmq

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

REQUEST_TIMEOUT = 5000
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5555"

class ZQueue(object):
    def __init__(self, backend_socket):
        self.frontend = ZMQStream(backend_socket)
        self.frontend.on_recv(self.handle_backend)
        self.frontend.send_string(str(1))
        self.loop = IOLoop.instance()

    def handle_backend(self,msg):
        m = msg[:]
        print(m)

    def run(self):
        try:
            self.loop.start()
        except KeyboardInterrupt:
            helpers.Helpers.times_str('ctrlc')

class Runner(object):
    def __init__(self):
        context = zmq.Context(1)
        self.backend = context.socket(zmq.ROUTER)
        self.backend.setsockopt_string(zmq.IDENTITY, 'Test-Client 001')
    def start(self,back):
        self.backend.bind(back)
        self.runner = ZQueue(self.backend)
        self.runner.run()
def main():
    r = Runner();
    print("I: Binding server…")
    r.start('tcp://*:5555');
        
if __name__ == "__main__":
    main()