from rpc.common import pickle, unpickle, RemoteException
from rpc.connection import BufferedConnection
from rpc.poller import EPollWorker
import logging
import select
import socket
import sys
import threading
import types
import time

SERVER_RPCID_GEN = iter(xrange(100000000))

class ServerHandle(object):
  def __init__(self, connection, method, args, client_rpcid):
    self.connection = connection
    self.method = method
    self.args = args
    self.start = time.time()
    self.server_rpcid = SERVER_RPCID_GEN.next()
    self.client_rpcid = client_rpcid
    self.done = None

  def elapsed(self):
    return time.time() - self.start


class ServerConnection(BufferedConnection):
  def __init__(self, rpc_server, socket, addr):
    logging.debug('Server (%s:%d) -- accepted connection from %s',
                  rpc_server.host, rpc_server.port, addr)
    self.rpc_server = rpc_server
    BufferedConnection.__init__(self, socket, addr)

  def read_finished(self):
#    logging.info('Read finished!')
    while self.has_message():
      rpcid, message = self.pop_message()
      method, args = unpickle(message)
      self.rpc_server._dispatch(ServerHandle(self, method, args, rpcid))

  def write_finished(self):
    pass

class RPCServer(threading.Thread):
  def __init__(self, host, port, handler):
    threading.Thread.__init__(self)
    self.setDaemon(True)
    self.host = host
    self.port = port
    self.handler = handler
    self.handler.server = self
    self.running = True

    self._pending_rpcs = {}

    self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self._listen_socket.settimeout(None)
    self._listen_socket.bind((host, port))

    self._poller = EPollWorker('Poll.%s.%d' % (handler.__class__.__name__, port))

  def num_pending_rpcs(self):
    return len(self._pending_rpcs)
  
  def run(self):
    self.serve_forever()
    
  def stop(self):
    self.shutdown()

  def shutdown(self):
#    self._call_histogram.dump()
    self.running = False
    logging.debug('Shutting down!')

  def serve_forever(self):
    self._listen_socket.listen(1000)
    while self.running:
      #logging.debug('Waiting for connections...')
      s = [self._listen_socket]
      try:
        r, w, _ = select.select(s, s, s, 0.1)
      except select.error:
        continue

      if r or w:
        client_socket, addr = self._listen_socket.accept()
        conn = ServerConnection(self, client_socket, addr)
        self._poller.register(conn)

    logging.info('Listen socket shutdown successfully.')
    self._poller.stop()
    self._listen_socket.close()

  def rpc_finished(self, server_rpcid, *result):
#    logging.info('RPCFINISHED %d', server_rpcid)
    try:
      handle = self._pending_rpcs[server_rpcid]
      logging.debug('Returning result for sid %d cid %d', handle.server_rpcid, handle.client_rpcid)
      if isinstance(result, tuple) and len(result) == 3 and isinstance(result[2], types.TracebackType):
        result = RemoteException(result)

      message = pickle((result, handle.elapsed(), None))
      handle.connection.push_message(handle.client_rpcid, message)
      del self._pending_rpcs[server_rpcid]
    except:
      logging.fatal('Unexpected error handling rpc finished!', exc_info = 1)
      time.sleep(1)

  def _dispatch(self, handle):
#    logging.info('RPCSTART %d', handle.server_rpcid)
#    logging.debug('Dispatching: %s', handle.method)
    logging.debug('Dispatching for sid %d cid %d',
                  handle.server_rpcid, handle.client_rpcid)
    self._pending_rpcs[handle.server_rpcid] = handle
    handle.done = lambda *args: self.rpc_finished(handle.server_rpcid, *args)
    handle.error = lambda exc_info: self.rpc_finished(handle.server_rpcid, RemoteException(exc_info))
    try:
      getattr(self.handler, handle.method)(handle, *handle.args)
    except:
      logging.warn('Exception while handling method: %s', handle.method, exc_info=1)
      handle.error(sys.exc_info())
#    logging.debug('Dispatch finished.')

