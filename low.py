import q, w
import zmq
import threading
import time
from base import *
from zmq.eventloop.ioloop import IOLoop
def main():
    context = zmq.Context(1)
    
    #qq = q.TheQueue(context, CLIENT_URL, WORKER_URL)
    qq = q.TheQueue(context, CLIENT_URL, WORKER_URL)
    for i in range(2):
        t = threading.Thread(target=w.TheWorker,args=(context,))
        t.start()
        #t.join()
    try:
        IOLoop.instance().start()
    except KeyboardInterrupt:
        times_str('ctrlc')

if __name__ == "__main__":
    main()