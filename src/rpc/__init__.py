import socket
import threading

class SocketError(socket.error): pass
class RPCError(Exception): pass

_DefaultPoller = None
def DefaultPoller():
  from rpc import poller
  global _DefaultPoller
  if not _DefaultPoller:
    _DefaultPoller = poller.EPollWorker('ClientPollWorker')
  return _DefaultPoller


def ResetPoller():
  from rpc import poller
  global _DefaultPoller
  _DefaultPoller = poller.EPollWorker('ClientPollWorker')


CLIENT_CACHE = threading.local()
def connect(host, port):
  from rpc.client import RPCClient
  if not hasattr(CLIENT_CACHE, 'connections'):
    CLIENT_CACHE.connections = {}

  if not (host, port) in CLIENT_CACHE.connections:
    CLIENT_CACHE.connections[(host, port)] = RPCClient(host, port)

  client = CLIENT_CACHE.connections[(host, port)]
  return client
