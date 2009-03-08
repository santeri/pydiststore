#!/usr/bin/env python
# encoding: utf-8

__all__ = ['http_port', 'multicast_addr', 'multicast_port', 'multicast_dst', 'multicast_timeout']

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(['diststored.cfg', '/usr/local/etc/diststored.cfg', '/etc/diststored.cfg'])
_http_port      = int(config.get("diststored", "http_port"))
_multicast_addr = config.get("diststored", "multicast_addr")
_multicast_port = int(config.get("diststored", "multicast_port"))
_multicast_dst  = (_multicast_addr, _multicast_port)
_multicast_timeout = float(config.get("diststored", "multicast_timeout"))

def http_port():
    return _http_port

def multicast_addr():
    return _multicast_addr

def multicast_port():
    return _multicast_port

def multicast_dst():
    return _multicast_dst

def multicast_timeout():
    return _multicast_timeout