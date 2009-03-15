# encoding: utf-8

from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET
from socket import SO_REUSEADDR, IPPROTO_UDP

has_reuseport = False
try:
    from socket import SO_REUSEPORT
    has_reuseport = True
except:
    pass

def debug_print(str):
    print str

def error_print(str):
    print str

def udpsocket():
    s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    if has_reuseport:
        s.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)
    return s

