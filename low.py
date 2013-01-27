import q, w
import zmq
import threading
import time
from base import *

def dummy(i):
    print('dummy {}'.format(i))
    time.sleep(100)

def main():
    context = zmq.Context(1)
    
    qq = q.TheQueue(context, CLIENT_URL, WORKER_URL)

    for i in range(1):
        t = threading.Thread(target=w.TheWorker,args=(context,))
        t.start()
        t.join()

if __name__ == "__main__":
    main()