#!/usr/bin/env python
# encoding: utf-8

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
import urllib
import socket
import sys

class RequestHandler(threading.Thread):
    """Listen for multicast request from the other nodes.
        
        Possible request are:
        - whohas [key] [ip] [port]
            A request from a node for a key.
        - keycount [ip] [port]
            A request for the number of keys we have.
            
    """
    def __init__(self, addr, port, shared):
        """Set up a multicast listen
            
            iface is the interface we should use for multicast.
            addr is the multicast address to use.
        """
        self.shared = shared
        s = udpsocket()
        s.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 10)
        s.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 0)
        s.bind(('', port))
        mreq = struct.pack("4sl", socket.inet_aton(multicast_addr), socket.INADDR_ANY)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        debug_print("Request handler at %s:%d in multicast group %s" % (addr, port, multicast_addr))
        self.sock = s
        threading.Thread.__init__(self)
    
    def run(self):
        """run the handler"""
        while True:
            msg= self.sock.recv(1024)
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
    

class HttpHandler(BaseHTTPRequestHandler):
    def request_key(self, key):
        """helper for sending key request"""
        # create socket and find a free port
        rsock, port = self.server.shared.pm.next_socket()
        rsock.settimeout(multicast_timeout)
        # send a request with sender & port information
        sock = udpsocket()
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        data = "whohas %s %s %d" % (key, self.server.shared.ip, port)
        debug_print("%s: sending multicast: %s" % (self.server.shared.ip, data))
        sock.sendto(data, multicast_dst)
        sock.close()
        # wait for a response or time out
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
        rsock, port = self.server.shared.pm.next_socket()
        rsock.settimeout(multicast_timeout)
        # send a request with sender & port information
        sock = udpsocket()
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        data = "keycount %s %d" % (self.server.shared.ip, port)
        debug_print("%s: sending multicast: %s" % (self.server.shared.ip, data))
        sock.sendto(data, multicast_dst)
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
                    print_error("%s: received invalid response: %s " % (self.server.shared.ip, sys.exc_info()))
                print resp
        except socket.error, e:
            pass # timeout
        rsock.close()
        # Return the server list sorted by the key count or ip
        servers.sort(cmp=lambda a,b: cmp(b[0], a[0]) or cmp(a[1], b[1]))
        return servers
        
    def do_GET(self):
        """Handle get requests.
            
        If the key doesn't exist locally we create a listening udp socket, 
        and the send a multicast request to the cluster.  Then we request 
        the value from the first responder and cache it locally and send the
        result to the client
        """
        debug_print("%s: http request for %s" % (self.server.shared.ip, self.path))
        
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
        else:
            if cmd == 'getlocal':
                # Don't check the cluster.
                self.send_error(404)
                return
                
            resp = self.request_key(key) 
            
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
                        (remote_ip, http_port, key), "GET")
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
        # master servers are decided by who has the most keys, and
        # are updated by periodically sending multicast status requests.
        
        # parse post data
        env = {'REQUEST_METHOD':'POST'}
        if 'Content-Type' in self.headers:
            env['CONTENT_TYPE'] = self.headers['Content-Type']
        form = cgi.FieldStorage(fp = self.rfile, headers = self.headers, environ = env)
        k, v = form['key'].value, form['value'].value

        self.server.shared.ds.put(k, v)

        if 'local' in self.path:  
            # Skip cluster update
            self.send_response(200)
            self.end_headers()
            return
        
        if self.server.shared.masters == []:
            # We have no masters, check if any are available.
            # Send a request, and wait a second for replies.
            servers = self.request_keycount()
            if servers != []:
                self.server.shared.masters = servers[:2]
                self.server.shared.nodes   = servers
                print "servers: ", servers
                print "found masters: ", self.server.shared.masters

        client = httplib2.Http()
        for _, server in self.server.shared.masters:
            if server != self.server.shared.ip: # no point in sending ourself our data.
                print "sending post to ", server
                # todo: don't use urlencoding
                resp, content = client.request("http://%s:%s/local/" % (server, http_port), "POST", 
                                               urllib.urlencode({'key': k, 'value': v}))
                print resp, content    
        
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
           so we do it here."""
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        HTTPServer.allow_reuse_address = 1
        HTTPServer.server_bind(self)
    

class Server(object):
    class Shared(object):
        """Holds shared data between the multicast request handler and
           the http server.
        """
        nodes = dict() # Contains host -> number of keys pairs
        masters = []
        
        def __init__(self, ds, pm, ip):
            self.ds = ds
            self.pm = pm
            self.ip = ip
        
    
    def __init__(self, ip='', ds = Datastore(), pm = Portmanager(50000,1000)):
        """create the http server and start the multicast thread"""
        shared = Server.Shared(ds, pm, ip)
        self.http = ThreadedHttpServer((ip, http_port), HttpHandler, shared)
        self.http.daemon_threads = True
        self.rh = RequestHandler(ip, multicast_port, shared)
        self.rh.setDaemon(True)
    
    def start(self):
        """start the servers main loops"""
        self.rh.start()
        self.http.serve_forever()
    

