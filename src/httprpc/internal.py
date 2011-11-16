#!/usr/bin/env python

from eventlet.green import httplib
from httprpc.common import Message
import logging
import types
import yaml

def load(data):
  '''Takes as input a string and returns a deserialized output object'''
  #logging.info('Loading: "%s"', data)
  return yaml.load(data)

def store(message):
  '''Takes as input an object and returns a serialized string representation.'''
  return yaml.dump(message)

class ServerRequest(Message):
  method = None
  args = None
  kw = None

class ServerResponse(Message):
  objectid = None
  data = None
  exception = None
  traceback = None

PRIMITIVES = set(
  [ types.IntType,
    types.CodeType,
    types.FloatType,
    types.BooleanType,
    types.StringType,
    types.NoneType, ])

def is_primitive(obj):
  if type(obj) in PRIMITIVES:
    return True

  if isinstance(obj, Message):
    return True

  if isinstance(obj, types.ListType):
    for v in obj:
      if not is_primitive(v):
        return False
    return True

  if isinstance(obj, types.DictType):
    for k, v in obj.iteritems():
      if not is_primitive(k) or not is_primitive(v):
        return False
    return True
  return False

class RPCError(Exception):
  pass

class ConnectionLost(RPCError):
  pass

class ServerError(RPCError):
  pass

class Channel(object):
  def __init__(self, host, port):
    self.host = host
    self.port = port

  def get(self, path, data):
    http = httplib.HTTPConnection(self.host, self.port)
    http.request('POST', path, data)
    return http.getresponse()

  def __repr__(self):
    return 'HTTP(%s:%s)' % (self.host, self.port)
