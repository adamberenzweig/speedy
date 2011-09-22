#!/usr/bin/env python

from httprpc import internal

class Client(object):
  def __init__(self, host, port):
    self._host = host
    self._port = port
    self._timeout = None

  def set_timeout(self, timeout):
    self._timeout = timeout

  def __repr__(self):
    return 'Client(%s, %d)'

  def __getattr__(self, k):
    if not k in self.__dict__:
      return RemoteObjectProxy(internal.Channel(self._host, self._port), k)
    raise KeyError


class CallStub(object):
  def __init__(self, channel, objectid, method):
    self._channel = channel
    self._objectid = objectid
    self._method = method

  def __call__(self, *args, **kw):
#    logging.info('Calling %s %s %s %s', self._objectid, self._method, args, kw)
    req = internal.ServerRequest(
      method = self._method,
      args = [internal.store(arg) for arg in args],
      kw = dict([(k, internal.store(v)) for (k, v) in kw]))

    path = '/rpc/invoke/%s' % self._objectid
    self._channel.request('POST', path, internal.store(req))
    resp = self._channel.getresponse()
    if resp.status != 200:
      raise internal.ServerError, 'Error connecting to %s%s -- %d' % (
        self._channel, path, resp.status)

    resp_data = resp.read(int(resp.getheader('content-length')))
    message = internal.load(resp_data)

#    logging.info('Response: %s', resp_data)

    assert isinstance(message, internal.ServerResponse)

    # the server returned an exception to us; reraise it
    if message.exc_info is not None:
      exc_info = message.exc_info
      tb = '\n' + '\n'.join(['%s -- %s' % (self._channel, line)
                             for line in exc_info.traceback.split('\n')])

      raise internal.ServerError, tb
    elif message.objectid:
      return RemoteObjectProxy(self._channel, message.objectid)
    else:
      return message.data

class RemoteObjectProxy(object):
  def __init__(self, channel, objectid):
    self._channel = channel
    self._objectid = objectid

  def __getattr__(self, k):
    return CallStub(self._channel, self._objectid, k)
