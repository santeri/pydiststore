# encoding: utf-8

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
    def _find_port(self, sock):
        """Try to find a free socket"""
        for _ in range(self.count):
            try:
                # if the bind fails, try the next port.
                sock.bind(('', self.current))
                return sock, self.current
            finally:
                self.current += 1
                if self.current - self.start > self.count:
                    self.current = self.start
        return False
    def next_socket(self):
        """Return the next free socket in the configured range
            
            Will not return until a socket becomes free.
        """
        sock = udpsocket()    
        while True:
            try:
                self.lock.acquire()
                r = self._find_port(sock)
                if r != False:
                    return r
            finally:
                self.lock.release()
            time.sleep(1)