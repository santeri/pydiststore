# encoding: utf-8

import unittest
import threading
import time
import httplib2
import BaseHTTPServer
import socket
import os

from diststore import *
from diststore.settings import *

class TestHttp(unittest.TestCase):
    def setUp(self):
        """docstring for setUp"""
        self.client = httplib2.Http()
        time.sleep(0.1)
        
        pid = os.fork()
        if pid:
            self.pid = pid
            # wait a bit so the server has time to start
        else:
            Server(ip="127.0.0.1").start()
        time.sleep(0.1)
    
    def testPost(self):
        """Try to post a new key/value pair"""
        resp, content = self.client.request("http://127.0.0.1:%d/testkey42" % http_port(), "POST", 'testvalue http post')
        self.assertEquals(resp.status, 200)
    
    def testGet(self):
        resp, content = self.client.request("http://127.0.0.1:%d/testkey42" % http_port(), "POST", 'testvalue http post')
        self.assertEquals(resp.status, 200)
        resp, content = self.client.request("http://127.0.0.1:%d/get/testkey42"%http_port(), "GET")
        self.assertEquals(resp.status, 200)
        self.assertEquals(content, "testvalue http post")
    
    def testMissingKey(self):
        url = "http://127.0.0.1:%d/get/nonexistingkey"%http_port()
        print url
        resp, content = self.client.request(url, "GET")
        self.assertEquals(resp.status, 404)
    
    def tearDown(self):
        try:
            os.kill(self.pid, 15)
            os.wait4(self.pid, 0)
        except OSError,e:
            pass # the child might already be dead..
    

class TestDataStore(unittest.TestCase):
    def setUp(self):
        """docstring for SetUp"""
        self.ds = Datastore()
    
    def testPut(self):
        """docstring for testPut"""
        self.ds.put("testkey", "testvalue")
        val = self.ds.get("testkey")
        self.assertEqual(val, "testvalue")
    
    def testHas(self):
        self.ds.put("testkey", "testvalue")
        self.assertTrue(self.ds.has("testkey"))
    
    def testThreadedPut(self):
        class PutThread(threading.Thread):
            def __init__(self, ds, n):
                """init our dummy thread"""
                threading.Thread.__init__(self)
                self.ds = ds
                self.n = n
            
            def run(self):
                """put a key to the datastore"""
                time.sleep(1.0) # sleep so all the threads have time to start
                self.ds.put(str(self.n), 'test value for %d' % self.n)
            
        
        nthreads = 500
        threads = [PutThread(self.ds, n) for n in range(nthreads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(len(self.ds.keys()), nthreads)
    

class TestPortmanager(unittest.TestCase):
    def setUp(self):
        self.pm = Portmanager(50000, 5000)
    
    def testPorts(self):
        """try to get 100 bound sockets"""
        sockets = [self.pm.next_socket() for _ in range(100)]
        self.assertEqual(len(sockets), 100)
    

class TestCluster(unittest.TestCase):
    pids = {}
    ip_fmt = "10.1.2.%d"
    nservers = 10
    
    def setUp(self):
        """Set up several nodes on different ips,
           This needs previous setup of ip addresses on localhost from 
           10.1.2.1 to 10.1.2.11.
        """
        time.sleep(0.1)
        for i in range(1, self.nservers+1):
            pid = os.fork()
            if pid:
                self.pids[i] = pid 
            else:
                # in child
                start = 50000 + i * 100
                count = 99
                #print "child %d starting with ports %d-%d" % (i, start, start+count)
                dhs = Server(ip=self.ip_fmt % i, pm=Portmanager(start, count))
                dhs.start()
        # wait a bit so the servers has time to start
        time.sleep(0.1)
        print self.pids
        
        self.client = httplib2.Http()
    
    def _checked_post(self, node, key, value, status=200):
        """docstring for _checked_put"""
        url = "http://%s:%s/%s" % (self.ip_fmt % node, http_port(), key)
        resp, content = self.client.request(url, "POST", value)
        self.assertEquals(resp.status, status)
    
    def _checked_get(self, node, key, cmd='get', status=200):
        """Do a get for `key` from `node` and check that status is `status`"""
        url = "http://%s:%s/%s/%s" % (self.ip_fmt % node, http_port(), cmd, key)
        resp, content = self.client.request(url, "GET")
        self.assertEquals(resp.status, status)
        return content
    
    def testGetFromAll(self):
        """Try to get from all servers."""
        for i in range(1, self.nservers+1):
            self._checked_get(i, "nonexistingkey", status=404)
    
    def testPutGet(self):
        """put a key to one server, get from another"""
        testkey, testvalue = 'testkey47', 'testvalue, testPutGet'
        self._checked_post(1, testkey, testvalue)
        value = self._checked_get(2, testkey)
        self.assertEquals(value, testvalue)
    
    def testMasterSelection(self):
        """Put keys on all servers and check that the master selection works"""
        for i in range(1, self.nservers+1):
            self._checked_post(i, 'testkey%d' % i, 'testvalue, for server %d' % i)
        
        # now we should have two servers with all the keys, check server 1.
        for i in range(1, self.nservers+1):
            # NOTE: need to use getlocal, or this wont work!
            value = self._checked_get(1, 'testkey%d' % i, cmd='getlocal')
            self.assertEquals(value, "testvalue, for server %d" % i)
    
    def testSync(self):
        """Check that syncing data works"""
        # put 10 keys to .1,  this will make .1 and .10 master servers.
        for i in range(1, 10):
            self._checked_post(1, 'testkey%d' % i, 'testvalue %d, for server 1' % i)
        
        # put 10 keys to .5 they will be sync'd to .1 and .10.
        for i in range(1, 10):
            self._checked_post(5, 'testkey-2-%d' % i, 'testvalue %d, for server 5' % i)
        
        # get all keys from .1.
        for i in range(1, 10):
            value = self._checked_get(1, 'testkey%d' % i, cmd='getlocal')
            self.assertEquals(value, 'testvalue %d, for server 1' % i)
            
            value = self._checked_get(1, 'testkey-2-%d' % i, cmd='getlocal')
            self.assertEquals(value, 'testvalue %d, for server 5' % i)
    
    def testClusterGet(self):
        """Check that syncing data works, part 2"""
        # put 10 keys to .1,  this will make .1 and .10 master servers.
        for i in range(1, 10):
            self._checked_post(1, 'testkey%d' % i, 'testvalue %d, for server 1' % i)
        
        # put 10 keys to .5 they will be sync'd to .1 and .10.
        for i in range(1, 10):
            self._checked_post(5, 'testkey-2-%d' % i, 'testvalue %d, for server 5' % i)
        
        # get all keys from .3 which will query cluster.
        for i in range(1, 10):
            value = self._checked_get(3, 'testkey%d' % i)
            self.assertEquals(value, 'testvalue %d, for server 1' % i)
            value = self._checked_get(3, 'testkey-2-%d' % i)
            self.assertEquals(value, 'testvalue %d, for server 5' % i)
        
        # now .3 should have all the keys
        for i in range(1, 10):
            value = self._checked_get(3, 'testkey%d' % i, cmd='getlocal')
            self.assertEquals(value, 'testvalue %d, for server 1' % i)
            
            value = self._checked_get(3, 'testkey-2-%d' % i, cmd='getlocal')
            self.assertEquals(value, 'testvalue %d, for server 5' % i)
    
    def testMasterFail(self):
        """Test that a new master is selected when one goes down
            
            Send keys to .1 so it and .10 will be a masters,
            send keys to .2 so it will become master, when .1 goes down.
            
            Kill .1.
            
            Send a key to .3, and check that .2 becomes master.
        """
        time.sleep(1)
        for i in range(1, 10):
            self._checked_post(1, 'testkey%d' % i, 'testvalue %d, for server 1' % i)

        for i in range(1, 10):
            self._checked_post(i, 'testkey-2-%d' % i, 'testvalue-2 %d, for server 1' % i)
        
        time.sleep(1)
        
        self._checked_post(2, 'testkeyfor2', 'testvalue for 2')
        time.sleep(1)
        
        pid = self.pids[1]
        print "killing ", pid
        print os.kill(pid, 15)
        print os.wait4(pid, 0)
        del(self.pids[1])
        time.sleep(1)
        
        self._checked_post(3, 'testkeyfor3', 'testvalue for 3')

        for i in range(1, 10):
            value = self._checked_get(2, 'testkey%d' % i, cmd='getlocal')
            self.assertEquals(value, 'testvalue %d, for server 1' % i)
        
        
    
    def tearDown(self):
        """Kill the nodes"""
        for pid in self.pids.values():
            try:
                os.kill(pid, 15)
                os.wait4(pid, 0)
            except OSError,e:
                pass # the child might already be dead..
    

if __name__ == '__main__':
    unittest.main()
    