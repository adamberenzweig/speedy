from rpc import RPCError, DefaultPoller
from rpc.common import pickle, unpickle, RemoteException
from rpc.connection import BufferedConnection
import logging
import socket
import time
import threading
import types
import traceback

CLIENT_RPCID_GEN = iter(xrange(100000000))

class ClientHandle(object):
  PENDING = 0
  SENT = 1
  SUCCESS = 2
  TIMED_OUT = 3

  def done(self):
    return self.state >= ClientHandle.SUCCESS

  def __init__(self, rpcid, deadline = 60.0):
    self.rpcid = rpcid
    self.state = ClientHandle.PENDING
    self.start_time = time.time()
    self.server_elapsed = 0.0
    self.end_time = None
    self.result = None
    self.error = None
    self.deadline = self.start_time + deadline
    self.finished_cv = threading.Condition()

  def __repr__(self):
    return 'ClientHandle(rpc=%d, state=%d)' % (self.rpcid, self.state)

class ClientConnection(BufferedConnection):
  def __init__(self, host, port):
    port = int(port)
    self._callbacks = {}
    self.pending_rpcs = {}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while 1:
      try:
        s.connect((host, port))
        break
      except socket.error, e:
        logging.warn('Error trying to connect to %s:%d, retrying in 1s. :: %s',
                     host, port, e)
        time.sleep(1)

    BufferedConnection.__init__(self, s, (host, port))
    DefaultPoller().register(self)


  def read_finished(self):
    while self.has_message():
      rpcid, blob = self.pop_message()
      result, server_elapsed, error = unpickle(blob)
      handle = self.pending_rpcs.get(rpcid, None)
      if not handle:
        logging.info('Dropping response to stale handle %s', rpcid)
      else:
        with handle.finished_cv:
          handle.server_elapsed = server_elapsed
          handle.result = result
          handle.error = error
          handle.state = ClientHandle.SUCCESS
          handle.end_time = time.time()
          self.invoke_callback(rpcid, handle)
          handle.finished_cv.notify_all()

  def invoke_callback(self, rpcid, handle):
    if rpcid in self._callbacks:
      cb = self._callbacks[rpcid]
      cb(handle.result)
      del self._callbacks[rpcid]
      del self.pending_rpcs[rpcid]

  def write_finished(self):
#    logging.info('Write finished!')
    pass

  def send(self, method, args, timeout = 60.0):
    logging.debug('Sending RPC %s', method)
    rpcid = CLIENT_RPCID_GEN.next()
    self.pending_rpcs[rpcid] = ClientHandle(rpcid, timeout)
    self.push_message(rpcid, pickle((method, args)))
    return rpcid

  def add_callback(self, rpcid, cb):
    self._callbacks[rpcid] = cb
    if self.pending_rpcs[rpcid].state == ClientHandle.SUCCESS:
      self.invoke_callback(rpcid, self.pending_rpcs[rpcid])

  def wait_for_response(self, rpcid):
#    logging.info('Waiting...')
    if not rpcid in self.pending_rpcs:
      raise RPCError, 'Tried to wait_for_response on a non-existing message.'

    handle = self.pending_rpcs[rpcid]
    with handle.finished_cv:
      if not handle.done():
        handle.finished_cv.wait(handle.deadline)

    if not handle.done():
      logging.warn('RPC %s (client %s, server %s:%d) timed out.',
                   handle.rpcid, socket.gethostname(), self.host, self.port)
      handle.state = ClientHandle.TIMED_OUT
      handle.end_time = time.time()
    
    del self.pending_rpcs[rpcid]

    if handle.error is not None:
      raise RPCError, handle.error

    return handle

class Future(object):
  def __init__(self, socket, rpcid):
    self.conn = socket
    self.rpcid = rpcid
    self.result = None
    self.sent = False
    self.handle = self.conn.pending_rpcs[rpcid]

  def __cmp__(self, other):
    return cmp(self.wait(), other)

  def __repr__(self):
    return 'Future for RPC(%s)' % self.rpcid

  def done(self):
    return self.handle.done()

  def start_time(self):
    return self.handle.start_time

  def end_time(self):
    return self.handle.end_time

  def server_elapsed(self):
    return self.handle.server_elapsed

  def state(self):
    return self.handle.state

  def elapsed(self):
    return self.handle.end_time - self.handle.start_time

  def set_callback(self, callback):
    self.conn.add_callback(self.rpcid, callback)

  def wait(self):
    if self.result is not None:
      return self.result

    self.sent = True
    handle = self.conn.wait_for_response(self.rpcid)
    result = handle.result
    
    # special case -- if the response only contains one argument, extract 
    # the argument and return
    if len(result) == 1: result = result[0]

    if handle.state != ClientHandle.SUCCESS:
      raise Exception('Call failed.')
    
    if isinstance(result, RemoteException):
      logging.error('Remote exception!\n: %s', result.formatted_tb)
      raise result.exception

    self.result = result
    return self.result

class _MethodCall(object):
    def __init__(self, socket, name):
      self.conn = socket
      self.name = name

    def __call__(self, *args, **kw):
      timeout = kw.get('timeout', 60)
      return Future(self.conn, self.conn.send(self.name, args, timeout = timeout))

class RPCClient(object):
  def __init__(self, host, port):
    self.host = host
    self.port = int(port)
    self.conn = ClientConnection(host, port)

  def __getattr__(self, key):
    return _MethodCall(self.conn, key)

  def close(self):
    del self.conn
