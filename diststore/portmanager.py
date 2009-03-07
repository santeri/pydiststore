# encoding: utf-8

from __future__ import with_statement
import time
import threading
from util import udpsocket

class Portmanager(object):
    """Handles allocation of udp socket bound to a port.
    """
    def __init__(self, start, count):
        """set up a list of ports
        
            start: first port
            count: max number of ports
        """
        self.start = start
        self.current = start
        self.count = count
        self.lock = threading.Lock()

    def next_socket(self):
        """Return the next free socket in the configured range
        
            Will not return until a socket becomes free.
        """
        sock = udpsocket()    
        while True:
            with self.lock:
                # Try to find a free socket
                for _ in range(self.count):
                    try:
                        # if the bind fails, try the next port.
                        sock.bind(('', self.current))
                        return sock, self.current
                    finally:
                        self.current += 1
                        if self.current - self.start > self.count:
                            self.current = self.start
            # No free sockets found, wait and retry
            time.sleep(1)
