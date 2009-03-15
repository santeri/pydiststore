#!/usr/bin/env python
# encoding: utf-8
has_with = False

# Bah.
#try:
#    from __future__ import with_statement
#    has_with = True
#except:
#    pass

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from portmanager import Portmanager
from datastore import Datastore
from util import *
from settings import *
import threading
import cgi
import struct
import httplib2
import socket
import sys

class SyncThread(threading.Thread):
    """Handles sync if we become a new master"""
    def __init__(self, shared):
        super(SyncThread, self).__init__()
        self.shared = shared
    def run(self):
        debug_print("syncthread getting masters")
        oldmasters = [e[1] for e in self.shared.masters]
        self.shared.update_masters()
        debug_print("syncthread got masters")
        newmasters = [e[1] for e in self.shared.masters]
        if self.shared.ip in oldmasters:
            # we are a master, don't do anything
            debug_print("%s: we are a master, don't sync" % self.shared.ip)
            return

        if self.shared.ip in newmasters:
            # get a list of keys from the other master
            # and fetch the ones we're missing.
            server = list(newmasters)
            server.remove(self.shared.ip)
            server = server[0]
            debug_print("%s: new master, starting sync from %s" % (self.shared.ip, server))
            
            client = httplib2.Http()
            resp, content = client.request("http://%s:%d/list" % (server, http_port()))
            if resp.status == 200:
                c = httplib2.Http()
                allkeys = content.split()
                debug_print("%s: server %s has %d keys" % (self.shared.ip, server, len(allkeys)))
                needkeys = [k for k in allkeys if self.shared.ds.has(k) == False]
                debug_print("%s: need to get %d keys" % (self.shared.ip, len(needkeys)))
                for key in needkeys:
                    r, v = c.request("http://%s:%d/getlocal/%s" % (server, http_port(), key))
                    if r.status == 200:
                        self.shared.ds.put(key, v)
                    else:
                        # Eek!
                        error_print("%s: server %s is missing key %s" % (self.shared.ip, server, key))
                debug_print("%s: new master, sync complete" % self.shared.ip)
            else:
                # Ook!
                error_print("%s: failed to get a list of keys from %s" % (self.shared.ip, server))

class RequestHandler(threading.Thread):
    """Listen for multicast request from the other nodes.
        
        Possible request are:
        - whohas [key] [ip] [port]
            A request from a node for a key.
        - keycount [ip] [port]
            A request for the number of keys we have.
        - masterlost [who] [master]
            `who` failed to update it's master at `master`.
    """
    def __init__(self, addr, port, shared):
        """Set up a multicast listen
            
            iface is the interface we should use for multicast.
            addr is the multicast address to use.
        """
        s = udpsocket()
        s.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 10)
        s.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 0)
        s.bind(('', port))
        mreq = struct.pack("4sl", socket.inet_aton(multicast_addr()), socket.INADDR_ANY)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        debug_print("Request handler at %s:%d in multicast group %s" % (addr, port, multicast_addr()))
        self.sock = s
        self.shared = shared
        threading.Thread.__init__(self)
    
    def run(self):
        """run the handler"""
        while True:
            msg = self.sock.recv(1024)
            debug_print("%s: received multicast msg: %s" % (self.shared.ip, repr(msg)))
            if msg.startswith('keycount'):
                _, ip, port = msg.split()
                udpsocket().sendto("keycount %s %d" % (self.shared.ip, self.shared.ds.count()), (ip, int(port)))
            elif msg.startswith('whohas'):
                _, key, ip, port = msg.split()
                if self.shared.ds.has(key):
                    # send yes with our ip
                    msg = "gotkey %s %s" % (key, self.shared.ip)
                    debug_print("%s: we have the key, responding with: %s" % (self.shared.ip, msg))
                    udpsocket().sendto(msg, (ip,int(port)))
                else:
                    # here we could send a negative response to keep
                    # latencies down. Good Idea?
                    pass
            elif msg.startswith('masterlost'):
                _, ip, master_ip = msg.split()
                if self.shared.syncthread != None and self.shared.syncthread.is_alive():
                    # hmm, this is not good, a sync is in progress and we have no way
                    # to stop it.
                    debug_print("syncthread already alive")
                else:
                    debug_print("starting sync thread")
                    st = SyncThread(self.shared)
                    st.setDaemon(True)
                    st.start()
    

class HttpHandler(BaseHTTPRequestHandler):
    """Handle http GET and POST requests.
        
        This class in instanced by the ThreadedHttpServer, once per connection
    """
    def do_GET(self):
        """Handle get requests.
            
        If the key doesn't exist locally we create a listening udp socket, 
        and the send a multicast request to the cluster.  Then we request 
        the value from the first responder and cache it locally and send the
        result to the client
        """
        debug_print("%s: http request for %s" % (self.server.shared.ip, self.path))
        if 'list' in self.path:
            self.send_response(200)
            self.end_headers()
            for key in self.server.shared.ds.keys():
                self.wfile.write("%s\n" % key)
            return
        
        if not 'get' in self.path:
            # invalid request
            self.send_error(404)
            return
            
        # parse request
        _, cmd, key = self.path.split('/') # /get/[key]
        # check if we have it locally
        if self.server.shared.ds.has(key):
            value = self.server.shared.ds.get(key)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(value)
            return
        else:
            if cmd == 'getlocal':
                # Don't check the cluster.
                self.send_error(404)
                return
            
            resp = self.server.shared.request_key(key) 
            
            if resp == None:
                self.send_error(404)
                return
            
            if resp.startswith('gotkey'):
                # gotkey [hash] [ip]
                _, remote_key, remote_ip = resp.split()
                if remote_key != key:
                    error_print("remote key does not match our request: %s != %s" % (remote_key, key))
                    self.send_error(500)
                
                debug_print("%s: received multicast: %s" % (self.server.shared.ip, resp))
                client = httplib2.Http()
                resp, content = client.request("http://%s:%s/get/%s" % 
                        (remote_ip, http_port(), key), "GET")
                if resp.status != 200:
                    error_print("%s: http get failed for key %s at host %s" % (self.server.shared.ip, key, remote_ip))
                    error_print("%s: %s" % (self.server.shared.ip, repr(resp)))
                    self.send_error(resp.status)
                
                # store the key locally
                self.server.shared.ds.put(key, content)
                # write the http response
                self.send_response(200)
                self.end_headers()
                self.wfile.write(content)
            else:
                debug_print("%s: received invalid response: %s" % (self.server.shared.ip, resp))
    
    def do_POST(self):
        """Store a key/value pair in the datastore, and send
            it to the master servers, if any.
        """
        
        # parse post data
        content_length = int(self.headers.get('Content-length', '-1'))
        v = self.rfile.read(content_length)
        k = self.path.split("/")[-1] # last part
        
        if k == "":
            # invalid request
            self.send_error(400)
            return
        
        # Store the key/value pair locally
        self.server.shared.ds.put(k, v)
        
        if 'local' in self.path:  
            # Skip cluster update
            self.send_response(200)
            self.end_headers()
            return
        
        if self.server.shared.masters == []:
            self.server.shared.update_masters()
        
        for _, server in self.server.shared.masters:
            if server != self.server.shared.ip: # no point in sending ourself our data.
                client = httplib2.Http()
                try:
                    url = "http://%s:%s/local/%s" % (server, http_port(), k)
                    print "sending localpost: ", url
                    resp, content = client.request(url, "POST", v)
                    if resp.status != 200:
                        print_error("%s: failed to update master at %s" % (self.server.shared.ip, server))
                        self.server.shared.notify_masterlost(self.server.shared.ip, server)
                except socket.error,e:
                    # Notify cluster that a master is down
                    # They key has been stored locally, and sent to the current
                    # master, if any.
                    self.server.shared.notify_masterlost(self.server.shared.ip, server)
        
        self.send_response(200)
        self.end_headers()
        self.wfile.write('post ok, data sent to %s\n' % repr(self.server.shared.masters))
        self.wfile.write('key: %s\nvalue:%s\n' % (k, v))
    

class ThreadedHttpServer(ThreadingMixIn, HTTPServer):
    """Handles request in a separate thread."""
    def __init__(self, addr, handler, shared):
        self.shared = shared
        HTTPServer.__init__(self, addr, handler)
    
    def server_bind(self):
        """Setting allow_reuse_address doesn't seem to work, 
           so we do it here.
        """
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # No reuseport in 2.4	
        #self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        HTTPServer.allow_reuse_address = 1
        HTTPServer.server_bind(self)
    

class Server(object):
    class Shared(object):
        """Holds shared data between the multicast request handler and
           the http server.
        """
        _lock = threading.Lock()
        _nodes = dict() # Contains host -> number of keys pairs
        _masters = []   # Top two nodes.
        
        syncthread = None
        
        def _getmasters(self):
            try:
                self._lock.acquire()
                return self._masters
            finally:
                self._lock.release()
        
        def _setmasters(self, new_masters):
            try:
                self._lock.acquire()
                self._masters = new_masters
            finally:
                self._lock.release()
        
        def _getnodes(self):
            try:
                self._lock.acquire()
                return self._nodes
            finally:
                self._lock.release()
        
        def _setnodes(self, new_nodes):
            try:
                self._lock.acquire()
                self._masters = new_nodes
            finally:
                self._lock.release()
        
        masters = property(_getmasters, _setmasters, doc="Set or get the masters list")
        nodes   = property(_getnodes,   _setnodes,   doc="Set or get the nodes list")
        
        def __init__(self, ds, pm, ip):
            self.ds = ds
            self.pm = pm
            self.ip = ip
        
        def update_masters(self):
            """update the node and master lists"""
            # We have no masters, check if any are available.
            # Send a request, and wait a second for replies.
            servers = self.request_keycount()
            if servers != []:
                self.masters = servers[:2]
                self.nodes   = servers
                print "servers: ", servers
                print "found masters: ", self.masters
        
        def notify_masterlost(self, reporting_ip, master_ip):
            """Notify the cluster that a master node went missing"""
            sock = udpsocket()
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 10)
            sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
            data = "masterlost %s %s" % (reporting_ip, master_ip)
            debug_print("%s: sending multicast: %s" % (self.ip, data))
            sock.sendto(data, multicast_dst())
            sock.close()
        
        def request_key(self, key):
            """helper for sending key request to the cluster"""
            # create socket and find a free port
            rsock, port = self.pm.next_socket()
            rsock.settimeout(multicast_timeout())
            # send a request with sender & port information
            sock = udpsocket()
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 10)
            sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
            
            data = "whohas %s %s %d" % (key, self.ip, port)
            debug_print("%s: sending multicast: %s" % (self.ip, data))
            sock.sendto(data, multicast_dst())
            sock.close()
            # wait for a response or time out
            try:
                try:
                    return rsock.recv(1024)
                except socket.error, e:
                    # timed out
                    return None
            finally:
                rsock.close()
        
        def request_keycount(self):
            """helper for sending keycount requests
            
            Sends a request for all nodes using multicast, 
            wait for responses for `multicast_timeout` and
            return the list of server, key count pairs, sorted
            by key count.
            """
            # create socket and find a free port
            rsock, port = self.pm.next_socket()
            rsock.settimeout(multicast_timeout())
            # send a request with sender & port information
            sock = udpsocket()
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
            
            data = "keycount %s %d" % (self.ip, port)
            debug_print("%s: sending multicast: %s" % (self.ip, data))
            sock.sendto(data, multicast_dst())
            sock.close()
            # wait for a response or time out
            servers = []
            try:
                while True:
                    # If no more responses are received in 1s, stop.
                    resp = rsock.recv(1024)
                    try:
                        cmd, ip, count = resp.split()
                        servers.append((int(count), ip))
                    except:
                        print_error("%s: received invalid response: %s " % (self.ip, sys.exc_info()))
                    print resp
            except socket.error, e:
                pass # timeout
            rsock.close()
            # Return the server list sorted by the key count and ip
            servers.sort(cmp=lambda a,b: cmp(b[0], a[0]) or cmp(a[1], b[1]))
            return servers
        
    
    def __init__(self, ip='', ds = Datastore(), pm = Portmanager(50000,1000)):
        """create the http server and start the multicast thread"""
        shared = Server.Shared(ds, pm, str(ip))
        self.http = ThreadedHttpServer((str(ip), http_port()), HttpHandler, shared)
        self.http.daemon_threads = True
        self.rh = RequestHandler(ip, multicast_port(), shared)
        self.rh.setDaemon(True)
    
    def start(self):
        """start the servers main loops"""
        self.rh.start()
        debug_print("server %s starting" % self.http.shared.ip)
        self.http.serve_forever()
    

