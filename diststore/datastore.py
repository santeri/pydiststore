# encoding: utf-8

from __future__ import with_statement
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
        with self.lock: self.ds[k] = v
    
    def has(self, k):
        """check for key `k`."""
        with self.lock: return k in self.ds
    
    def get(self, k):
        """get a value from the store with key `k`."""
        with self.lock: return self.ds[k]
    
    def keys(self):
        """get all keys in the store."""
        with self.lock:
            # Not really needed with python 2.5 as dict.keys() is atomic, 
            # but will be with python 3 where it won't be.
            return list(self.ds.keys())
    
    def count(self):
        """return the number of keys stored."""
        with self.lock: return len(self.ds.keys())
    
    def clear(self):
        """Remove all keys and values."""
        with self.lock:
            self.ds.clear()
