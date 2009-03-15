# encoding: utf-8

import threading

class Datastore(object):
    """Simple data store using a python dict.
        
        [todo: implement a variant which persists to disk]
        thread safe.
    """
    def __init__(self):
        """create datastore."""
        self.ds   = dict()
        self.lock = threading.Lock()
        
    def put(self, k, v):
        """store a key/value pair."""
        try:
            self.lock.acquire()
            self.ds[k] = v
        finally:
            self.lock.release()
    
    def has(self, k):
        """check for key `k`."""
        try:
            self.lock.acquire()
            return k in self.ds
        finally:
            self.lock.release()
            
    def get(self, k):
        """get a value from the store with key `k`."""
        try:
            self.lock.acquire()
            return self.ds[k]
        finally:
            self.lock.release()
    
    def keys(self):
        """get all keys in the store."""
        try:
            self.lock.acquire()
            # list(.. ) is not really needed with python 2.5 as dict.keys() is
            # atomic, but will be with python 3 where it won't be.
            return list(self.ds.keys())
        finally:
            self.lock.release()
    
    def count(self):
        """return the number of keys stored."""
        try:
            self.lock.acquire()
            return len(self.ds.keys())
        finally:
            self.lock.release()
    
    def clear(self):
        """Remove all keys and values."""
        try:
            self.lock.acquire()
            self.ds.clear()
        finally:
            self.lock.release()
