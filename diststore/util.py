# encoding: utf-8

from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET
from socket import SO_REUSEADDR, SO_REUSEPORT, IPPROTO_UDP

def debug_print(str):
    print str

def error_print(str):
    print str

def udpsocket():
    s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    s.setsockopt(SOL_SOCKET, SO_REUSEPORT, 1)
    return s

