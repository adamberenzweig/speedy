#!/usr/bin/env python

'''A simple HTTP based RPC server.

Usage:
class MyObject():
  def a(self): return Foo()

s = Server(host = '0.0.0.0', port = 9999)
s.register_object('test', MyObject())

c = Client(server_host, 9999)
foo = c.test.a()
'''

from eventlet import greenpool, sleep, greenthread, StopServe
from eventlet.green import BaseHTTPServer
from httprpc import internal
import eventlet
import logging
import sys
import traceback

class HTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  def log_message(self, fmt, *args):
    logging.info('[%s] -- %s', self.address_string(), fmt % args)

  def reply(self, code, data=''):
    self.send_response(code)
    self.send_header('content-length', len(data))
    self.end_headers()
    self.wfile.write(data)

  def process_request(self, data=None):
    handler = self.server.handler_for_path(self.path)
    if not handler:
      self.reply(404, '')
      return

    try:
      self.reply(200, handler.run(self.path, self.headers, data))
    except:
      self.reply(500, '')


  def do_POST(self):
    self.process_request(
      data=self.rfile.read(int(self.headers.getheader('content-length'))))

  def do_GET(self):
    self.process_request()


class ObjectRMI(object):
  def __init__(self, server, obj):
    self._server = server
    self._object = obj

  def run(self, path, headers, data):
#    logging.info('Running %s', path)
    try:
      req = internal.load(data)
      method = req.method
      args = tuple([internal.load(arg) for arg in req.args])
      kw = dict([(k, internal.load(v)) for (k, v) in req.kw.items()])

      local_resp = getattr(self._object, method)(*args, **kw)
      if local_resp is None:
        logging.warning('RMI call %s.%s returned no result.',
                        self._object.__class__.__name__, method)

      if internal.is_primitive(local_resp):
        resp = internal.ServerResponse(data=local_resp)
      else:
        objid = 'anonid:%s' % id(local_resp)
        self._server.register_object(objid, local_resp)
        resp = internal.ServerResponse(objectid=objid)
    except Exception:
      exc, message, _ = sys.exc_info()
      resp = internal.ServerResponse(
        exc_info=internal.ExceptionInfo(
          exception=str(exc),
          message=str(message),
          traceback=traceback.format_exc()))
    finally:
      return internal.store(resp)

try:
  import mako.lookup
  import mako.template

  class MakoRequestHandler(object):
    def __init__(self, tfile, **template_vars):
      self._tfile = tfile
      self._dict = template_vars

    def run(self, path, headers, data):
      lookup = mako.lookup.TemplateLookup(directories='')
      template = mako.template.Template(filename=self._tfile, lookup=lookup)
      return template.render(**self._dict)
except:
  MakoRequestHandler = None


class Server(object):
  def __init__(self,
               host='0.0.0.0',
               port=None,
               objects=None):
    assert(port != None)

    self._addr = (host, port)
    self._serving_thread = None
    self._clients = set()
    self._pool = greenpool.GreenPool(10000)
    self._handlers = {}

    if objects:
      for name, obj in objects.items():
        self.register_object(name, obj)

    self.register('/rpc/invoke/self', ObjectRMI(self, self))

  def register_object(self, name, obj):
    self.register('/rpc/invoke/%s' % name, ObjectRMI(self, obj))

  def register(self, path, handler):
    if not self._handlers.has_key(path):
      logging.info('Registering handler for %s', path)
      self._handlers[path] = handler

  def host(self): return self._addr[0]
  def port(self): return self._addr[1]

  def listen(self):
    self._socket = eventlet.listen(self._addr, backlog=1000)

  def handler_for_path(self, path):
    return self._handlers.get(path, None)

  def start(self):
    '''Start serving for this class in a separate thread.'''
    logging.info('Server starting up on %s:%s', self.host(), self.port())
    self.listen()
    self._serving_thread = eventlet.greenthread.spawn(self.__serve)
    # Let the serving thread begin accepting.
    sleep(0)
    return self._serving_thread

  def stop(self):
    for c in list(self._clients):
      logging.info('Terminating client %s.', c)
      greenthread.kill(c.thread, StopServe)
    self._pool.waitall()

    try:
      logging.debug('Killing server.')
      greenthread.kill(self._serving_thread, StopServe)
      self._serving_thread.wait()
    except StopServe:
      logging.debug('Killed.')

    self._serving_thread = None
    self._socket.close()

    logging.info('Stopped serving on: %s', self._addr)

  def __handle_request(self, csock, addr):
    HTTPRequestHandler(csock, addr, self)

  def __serve(self):
    assert self._socket, 'listen() must be called before serve.'

    while 1:
      try:
#        logging.info('Accepting...')
        csock, addr = self._socket.accept()
      except StopServe:
        logging.debug('Stopping serving on %s', self._addr)
        break

#      logging.info('Accepted connection.')
      self._pool.spawn(self.__handle_request, csock, addr)
      csock, addr = None, None


