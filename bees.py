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
    def __init__(self, frontend_socket):
        self.frontend = ZMQStream(frontend_socket)
        self.frontend.on_recv(self.handle_frontend)
        self.frontend.send_string(str(1))
        self.loop = IOLoop.instance()

    def handle_frontend(self,msg):
        m = msg[:]
        print(m)

    def handle_input(self):
        print('okay')

def main():
    context = zmq.Context(1)
    print("I: Connecting to server…")
    client_socket = context.socket(zmq.REQ)
    client_socket.setsockopt_string(zmq.IDENTITY, 'Test-Client 001')
    client_socket.connect(SERVER_ENDPOINT)

    queue = ZQueue(client_socket)

    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        print('ctrlc')
        
if __name__ == "__main__":
    main()