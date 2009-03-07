# encoding: utf-8

import unittest
import threading
import time
import httplib2
import BaseHTTPServer
import urllib
import socket
import os

from diststore import *
from diststore.settings import *

class TestHttp(unittest.TestCase):
    def setUp(self):
        """docstring for setUp"""
        self.client = httplib2.Http()
        pid = os.fork()
        if pid:
            self.pid = pid
        else:
            server = Server(ip="127.0.0.1")
            server.setDaemon(True)
            server.start()
        
    def testPost(self):
        """Try to post a new key/value pair"""
        data = {'key': 'testkey42', 'value': 'testvalue http post'}
        resp, content = self.client.request("http://127.0.0.1:%s/" % http_port, "POST", urllib.urlencode(data))
        self.assertEquals(resp.status, 200)
        print content
    def testGet(self):
        """docstring for testGet"""
        data = {'key': 'testkey42', 'value': 'testvalue http post'}
        resp, content = self.client.request("http://127.0.0.1:%s/" % http_port, "POST", urllib.urlencode(data))
        self.assertEquals(resp.status, 200)
        resp, content = self.client.request("http://127.0.0.1:%s/get/testkey42"%http_port, "GET")
        self.assertEquals(resp.status, 200)
        self.assertEquals(content, "testvalue http post")
    def testMissingKey(self):
        resp, content = self.client.request("http://127.0.0.1:%s/get/nonexistingkey"%http_port, "GET")
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
        """docstring for setUp"""
        self.pm = Portmanager(50000, 5000)
    def testPorts(self):
        """try to get 100 bound sockets
        """
        sockets = [self.pm.next_socket() for _ in range(100)]
        self.assertEqual(len(sockets), 100)

class TestCluster(unittest.TestCase):
    pids = []
    ip_fmt = "10.1.2.%d"
    nservers = 10
    
    def setUp(self):
        """Set up several nodes on different ips,
           This needs previous setup of ip addresses on localhost from 
           10.1.2.1 to 10.1.2.11.
        """
        for i in range(1, self.nservers+1):
            pid = os.fork()
            if pid:
                self.pids.append(pid)
            else:
                # in child
                start = 50000 + i * 100
                count = 99
                print "child %d starting with ports %d-%d" % (i, start, start+count)
                dhs = Server(ip=self.ip_fmt % i, pm=Portmanager(start, count))
                dhs.start()
        
        self.client = httplib2.Http()
        
                
    def testGetFromAll(self):
        """Try to get from all servers."""
        for i in range(1, self.nservers+1):
            url = "http://%s:%s/get/nonexistingkey" % (self.ip_fmt % i, http_port)
            print "request to %s" % url
            try:
                resp, content = self.client.request(url, "GET")
                self.assertEquals(resp.status, 404)
            except:
                pass
    def testPutGet(self):
        """put a key to one server, get from another"""            
        put_url = "http://%s:%s/" % (self.ip_fmt % 1, http_port)
        data = {'key': 'testkey47', 'value': 'testvalue, http post'}
        resp, content = self.client.request(put_url, "POST", urllib.urlencode(data))
        print "%s, %s" % (resp,content)
        self.assertEquals(resp.status, 200)
        
        get_url = "http://%s:%s/get/testkey47" % (self.ip_fmt % 2, http_port)
        resp, content = self.client.request(get_url, "GET")
        print "%s, %s" % (resp,content)
        self.assertEquals(resp.status, 200)
        self.assertEquals(content, data['value'])
    def testMasterSelection(self):
        """Put keys on all servers and check that the master selection works
        """
        for i in range(1, self.nservers+1):
            put_url = "http://%s:%s/" % (self.ip_fmt % i, http_port)
            data = {'key': 'testkey%d' % i, 'value': 'testvalue, for server %d' % i}
            resp, content = self.client.request(put_url, "POST", urllib.urlencode(data))
            print "%s, %s" % (resp,content)
            self.assertEquals(resp.status, 200)

        # now we should have two servers with all the keys, check server 1.
        for i in range(1, self.nservers+1):
            # NOTE: need to use getlocal, or this wont work!
            get_url = "http://%s:%s/getlocal/testkey%d" % (self.ip_fmt % 1, http_port, i)
            resp, content = self.client.request(get_url, 'GET')
            self.assertEquals(resp.status, 200)
            self.assertEquals(content, "testvalue, for server %d" % i)
    
    def testMultipartPost(self):
        """Check if form multipart encoding works, with big value"""
        self.assertEquals(False, True)
            
    def tearDown(self):
        """Kill the nodes"""
        for pid in self.pids:
            try:
                os.kill(pid, 15)
                os.wait4(pid, 0)
            except OSError,e:
                pass # the child might already be dead..
        
if __name__ == '__main__':

    unittest.main()        