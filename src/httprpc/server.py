#!/usr/bin/env python

from eventlet import greenpool, sleep, greenthread, StopServe
from eventlet.green import BaseHTTPServer
from httprpc import proto
from httprpc.common import ServerErrorResponse
from httprpc.proto import Message
from httprpc import log
import eventlet
import mako.lookup
import mako.template
import sys
import traceback

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  def log_message(self, fmt, *args):
    log.info('[%s] -- %s', self.address_string(), fmt % args)

  def reply(self, code, data = ''):
    self.send_response(code)
    self.send_header('content-length', len(data))
    self.end_headers()
    self.wfile.write(data)

  def do_POST(self):
    response = ''
    code = 500
    try:
      mlength = int(self.headers.getheader('content-length'))
      data = self.rfile.read(mlength)

      response = self.server.handlers[self.path].run(
        path = self.path, headers = self.headers, data = data)
      code = 200
    except KeyError:
      self.send_response(404, '')
    finally:
      self.reply(code, response)

  def do_GET(self):
    response = ''
    code = 500
    try:
      response = self.server.handlers[self.path].run(
        path = self.path, headers = self.headers, data = None)
      code = 200
    except KeyError:
      self.send_response(404, '')
    finally:
      self.reply(code, response)

class MakoRequestHandler(object):
  def __init__(self, tfile, **template_vars):
    self._tfile = tfile
    self._dict = template_vars

  def run(self, path, headers, data):
    lookup = mako.lookup.TemplateLookup(directories = '')
    template = mako.template.Template(filename = self._tfile, lookup = lookup)
    c = template.render(**self._dict)
    log.info('Content: %s', c)
    return c


class RPCRequestHandler(object):
  def __init__(self, server, method, request_class, response_class):
    self._server = server
    self._method = method
    self._req_class = request_class
    self._resp_class = response_class

  def run(self, path, headers, data):
    try:
      req = proto.load(data)
      assert isinstance(req, self._req_class)
      resp = self._server.invoke(self._method, req)
      assert isinstance(resp, self._resp_class)
      return proto.store(resp)
    except Exception:
      exc, message, _ = sys.exc_info()
      return proto.store(
        ServerErrorResponse(exception = str(exc),
                            message = str(message),
                            traceback = traceback.format_exc()))


class Server(object):
  METHODS = {}

  def __init__(self, host = '0.0.0.0', port = None):
    assert(port != None)

    self._addr = (host, port)
    self._serving_thread = None
    self._clients = set()
    self._pool = greenpool.GreenPool(10000)

    self.handlers = {}
    self.register_rpc_handlers()

  def register_rpc_handlers(self):
    for k, (req, resp) in self.METHODS.items():
      self.register('/rpc/%s' % k, RPCRequestHandler(self, k, req, resp))

  def register(self, path, handler):
    log.info('Registering handler for %s', path)
    self.handlers[path] = handler

  def host(self): return self._addr[0]
  def port(self): return self._addr[1]

  def listen(self):
    self._socket = eventlet.listen(self._addr, backlog = 1000)

  def start(self):
    '''Start serving for this class in a separate thread.'''
    log.info('Server starting up on %s:%s', self.host(), self.port())
    self.listen()
    self._serving_thread = eventlet.greenthread.spawn(self.__serve)
    # Let the serving thread begin accepting.
    sleep(0)
    return self._serving_thread

  def stop(self):
    for c in list(self._clients):
      log.info('Terminating client %s.', c)
      greenthread.kill(c.thread, StopServe)
    self._pool.waitall()

    try:
      log.debug('Killing server.')
      greenthread.kill(self._serving_thread, StopServe)
      self._serving_thread.wait()
    except StopServe:
      log.debug('Killed.')

    self._serving_thread = None
    self._socket.close()

    log.info('Stopped serving on: %s', self._addr)

  def invoke(self, method, request):
    resp = getattr(self, method)(request)
    assert resp is not None
    assert isinstance(resp, Message)
    return resp

  def __handle_request(self, csock, addr):
    RequestHandler(csock, addr, self)

  def __serve(self):
    assert self._socket, 'listen() must be called before serve.'

    while 1:
      try:
#        log.info('Accepting...')
        csock, addr = self._socket.accept()
      except StopServe:
        log.debug('Stopping serving on %s', self._addr)
        break

#      log.info('Accepted connection.')
      self._pool.spawn(self.__handle_request, csock, addr)
      csock, addr = None, None

