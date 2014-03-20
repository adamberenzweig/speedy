'''
Simple RPC library.

The :class:`.Client` and :class:`.Server` classes here work with
sockets which should implement the :class:`.Socket` interface.
'''
from cPickle import PickleError
import collections
import weakref
import sys
import threading
import time
import traceback
import types
import cStringIO
import cPickle

from . import config, util

RPC_ID = xrange(1000000000).__iter__()

CLIENT_PENDING = weakref.WeakKeyDictionary()
SERVER_PENDING = weakref.WeakKeyDictionary()

DEFAULT_TIMEOUT = 100
def set_default_timeout(seconds):
  global DEFAULT_TIMEOUT
  DEFAULT_TIMEOUT = seconds
  util.log_info('Set default timeout to %s seconds.', DEFAULT_TIMEOUT)


class RPCException(object):
  py_exc = None
  def __init__(self, py_exc):
    self.py_exc = py_exc

class PickledData(object):
  '''
  Helper class: indicates that this message has already been pickled,
  and should be sent as is, rather than being re-pickled.
  '''
  def __init__(self, data):
    self.data = data

class SocketBase(object):
  def send(self, blob): pass
  def recv(self): pass
  def flush(self): pass
  def close(self): pass

  def register_handler(self, handler):
    'A handler() is called in response to read requests.'
    self._handler = handler

  # client
  def connect(self): pass

  # server
  def bind(self): pass


def capture_exception(exc_info=None):
  if exc_info is None:
    exc_info = sys.exc_info()
  tb = traceback.format_exception(*exc_info)
  return RPCException(py_exc=''.join(tb).replace('\n', '\n:: '))


class Group(tuple):
  pass

def serialize_to(obj, writer):
  #serialization.write(obj, writer)
  try:
    pickled = cPickle.dumps(obj, -1)
    writer.write(pickled)
  except (PickleError, TypeError):
    #util.log_warn('CPICKLE failed: %s (%s)', sys.exc_info(), obj)
    writer.write(cloudpickle.dumps(obj, -1))

def serialize(obj):
  #x = cStringIO.StringIO()
  #serialization.write(obj, x)
  #return x.getvalue()
  try:
    return cPickle.dumps(obj, -1)
  except (PickleError, TypeError):
    return cloudpickle.dumps(obj, -1)

def read(f):
  return cPickle.load(f)

NO_RESULT = object()

class PendingRequest(object):
  '''An outstanding RPC request.

  Call done(result) when a method is finished processing.
  '''
  def __init__(self, socket, rpc_id):
    self.socket = socket
    self.rpc_id = rpc_id
    self.created = time.time()
    self.finished = False
    self.result = NO_RESULT

    SERVER_PENDING[self] = 1

  def wait(self):
    while self.result is NO_RESULT:
      time.sleep(0.001)
    return self.result

  def done(self, result=None):
    # util.log_info('RPC finished in %.3f seconds' % (time.time() - self.created))
    self.finished = True
    self.result = result

    if self.socket is not None:
      header = { 'rpc_id' : self.rpc_id }
      # util.log_info('Finished %s, %s', self.socket.addr, self.rpc_id)
      w = cStringIO.StringIO()
      cPickle.dump(header, w, -1)
      serialize_to(result, w)
      self.socket.send(w.getvalue())

  def __del__(self):
    if not self.finished:
      util.log_error('PendingRequest.done() not called before destruction (likely due to an exception.)')
      self.done(result=RPCException(py_exc='done() not called on request.'))


class RemoteException(Exception):
  '''Wrap a uncaught remote exception.'''
  def __init__(self, tb):
    self._tb = tb

  def __repr__(self):
    return 'RemoteException:\n' + self._tb

  def __str__(self):
    return repr(self)

class FnFuture(object):
  '''Chain ``fn`` to the given future.

  ``self.wait()`` return ``fn(future.wait())``.
  '''
  def __init__(self, future, fn):
    self.future = future
    self.fn = fn
    self.result = None

  def wait(self):
    result = self.future.wait()
    # util.log_info('Applying %s to %s', self.fn, result)
    self.result = self.fn(result)
    return self.result

class Future(object):
  def __init__(self, addr, rpc_id):
    self.addr = addr
    self.rpc_id = rpc_id
    self.have_result = False
    self.result = None
    self.finished_fn = None
    self._cv = threading.Condition()
    self._start = time.time()
    self._deadline = time.time() + DEFAULT_TIMEOUT

    CLIENT_PENDING[self] = 1

  def _set_result(self, result):
    self._cv.acquire()
    self.have_result = True

    if self.finished_fn is not None:
      self.result = self.finished_fn(result)
    else:
      self.result = result

    self._cv.notify()
    self._cv.release()

  def timed_out(self):
    return self._deadline < time.time()

  def handle_exc(self, exc):
    if config.throw_remote_exceptions:
      raise exc
    else:
      util.log_info('Remote host threw an exception (ignored)')
      util.log_info(exc)
      return None

  def wait(self):
    self._cv.acquire()
    while not self.have_result and not self.timed_out():
      # use a timeout so that ctrl-c works.
      self._cv.wait(timeout=0.1)
    self._cv.release()

#    util.log_info('Result from %s in %f seconds.', self.addr, time.time() - self._start)

    if not self.have_result and self.timed_out():
      return self.handle_exc(Exception('Timed out on remote call (%s %s)', self.addr, self.rpc_id))

    if isinstance(self.result, RPCException):
      return self.handle_exc(RemoteException(self.result.py_exc))

    return self.result

  def on_finished(self, fn):
    return FnFuture(self, fn)


class DummyFuture(object):
  def __init__(self, base=None):
    self.v = base

  def wait(self):
    return self.v

DUMMY_FUTURE = DummyFuture()

class FutureGroup(list):
  def wait(self):
    return [f.wait() for f in self]

def wait_for_all(futures):
  result = []
  for idx, f in enumerate(futures):
    result.append(f.wait())

  return result
  return [f.wait() for f in futures]


def run_rpc(server, rpc_name, rpc, req):
  # Python can't pickle bound methods, so we can't pass the result of getattr (a bound method)
  # to pool.run_async.  Instead, pass the object and rpc name and bind here.
  rpc_handler = getattr(server, rpc_name)
  rpc_handler(rpc, req)


class Server(object):
  def __init__(self, socket, thread_pool=None):
    self._socket = socket
    self._socket.register_handler(self.handle_read)
    self._running = False
    self._thread_pool = thread_pool

  def diediedie(self, handle, req):
    handle.done(None)
    self._socket.flush()
    self.shutdown()

  @property
  def addr(self):
    return self._socket.addr

  def serve(self):
    self.serve_nonblock()
    while self._running:
      time.sleep(0.1)

  def serve_nonblock(self):
#    util.log_info('Running.')
    self._running = True
    self._socket.bind()
  
  def handle_read(self, socket):
    #util.log_info('Reading...')

    data = socket.recv()
    reader = cStringIO.StringIO(data)
    header = cPickle.load(reader)

    #util.log_info('Reading: %s %s', self._socket.addr, header['rpc_id'])
    handle = PendingRequest(socket, header['rpc_id'])
    rpc_name = header['method']

    # This is basically equivalent to hasattr, but we want to handle the exception here.
    try:
      rpc_handler = getattr(self, rpc_name)
    except KeyError:
      handle.done(capture_exception())
      return

    try:
      # Run the handler on the process pool, not on this thread.
      # Handler is responsible for calling handle.done on the result.
      # TODO(madadam): Maybe better to wrap this in a function that runs the handler and then
      # calls handle.done() so the programmer can't forget.  The rpc handlers must return a
      # (result, status) tuple, and we can check the value in the wrapper.  Would need to change
      # a bunch of code that calls handle.done() early on exceptions/errors.
      req = read(reader)
      # TODO(madadam): put the req into the PendingRequest object, stop passing around both.
      args = (self, rpc_name, handle, req)
      if self._thread_pool:
        self._thread_pool.apply_async(run_rpc, args)
      else:
        run_rpc(*args)
    except:
      util.log_info('Exception in handle_read.', exc_info=1)
      handle.done(capture_exception())

  def shutdown(self):
    self._running = 0
    if self._threadpool:
      self._threadpool.close()
      self._threadpool.join()
    self._socket.close()
    del self._socket


class ProxyMethod(object):
  def __init__(self, client, method):
    self.client = client
    self.socket = client._socket
    self.method = method

  def __call__(self, request=None):
    rpc_id = RPC_ID.next()

    header = { 'method' : self.method, 'rpc_id' : rpc_id }

    f = Future(self.socket.addr, rpc_id)
    self.client._futures[rpc_id] = f

    w = cStringIO.StringIO()
    cPickle.dump(header, w, -1)

    if isinstance(request, PickledData):
      w.write(request.data)
    else:
      serialize_to(request, w)

    #util.log_info('Sending %s', self.method)
#    if len(serialized) > 800000:
#      util.log_info('%s::\n %s; \n\n\n %s', self.method, ''.join(traceback.format_stack()), request)

    self.socket.send(w.getvalue())
    return f

class Client(object):
  def __init__(self, socket):
    self._socket = socket
    self._socket.register_handler(self.handle_read)
    self._socket.connect()
    self._futures = {}

  def __reduce__(self, *args, **kwargs):
    raise cPickle.PickleError('Not pickleable.')

  def addr(self):
    return self._socket.addr

  def close(self):
    self._socket.close()

  def __getattr__(self, method_name):
    return ProxyMethod(self, method_name)

  def handle_read(self, socket):
    data = socket.recv()
    reader = cStringIO.StringIO(data)
    header = cPickle.load(reader)
    resp = read(reader)
    #resp = cPickle.load(reader)
    rpc_id = header['rpc_id']
    f = self._futures[rpc_id]
    f._set_result(resp)
    del self._futures[rpc_id]

  def close(self):
    self._socket.close()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    util.log_info('Closing socket...')
    self.close()


def forall(clients, method, request):
  '''Invoke ``method`` with ``request`` for each client in ``clients``

  ``request`` is only serialized once, so this is more efficient when
  targeting multiple workers with the same data.

  Returns a future wrapping all of the requests.
  '''
  futures = []
  pickled = PickledData(data=serialize(request))
  for c in clients:
    futures.append(getattr(c, method)(pickled))

  return FutureGroup(futures)

