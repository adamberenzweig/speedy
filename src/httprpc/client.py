#!/usr/bin/env python

from eventlet.green import httplib
from httprpc import proto
from httprpc.common import ServerErrorResponse, ServerError

class Client(object):
  class Stub(object):
    def __init__(self, host, port, method):
      self.host = host
      self.port = port
      self.http = httplib.HTTPConnection(host, port)
      self.method = method

    def __call__(self, message):
      message.validate()
      self.http.request('POST', '/rpc/%s' % self.method, proto.store(message))
#      log.info('Sent request...')
      resp = self.http.getresponse()
      message = proto.load(resp.read(int(resp.getheader('content-length'))))
#      log.info('Got response...')

      # the server returned an exception to us; reraise it
      if isinstance(message, ServerErrorResponse):
        # prefix each line with the server host:port
        tb = '\n' + '\n'.join(['%s:%s -- %s' % (self.host, self.port, line)
                               for line in message.traceback.split('\n')])

        raise ServerError, tb

      return message

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
      return Client.Stub(self._host, self._port, k)
    raise KeyError
