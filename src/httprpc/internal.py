#!/usr/bin/env python

from eventlet.green import httplib
import types
import yaml

def load(data):
  '''Takes as input a string and returns a deserialized output object'''
  return yaml.load(data)

def store(obj):
  '''Takes as input an object and returns a serialized string representation.'''
  return yaml.dump(obj)

class Message(object):
  def __init__(self, **kw):
    for k, v in kw.iteritems():
      setattr(self, k, v)

class ExceptionInfo(Message):
  exception = None
  traceback = None

class ServerRequest(Message):
  method = None
  args = None
  kw = None

class ServerResponse(Message):
  objectid = None
  data = None
  exc_info = None

PRIMITIVES = set(
  [ types.IntType,
    types.FloatType,
    types.BooleanType,
    types.StringType,
    types.NoneType ])

def is_primitive(obj):
  if type(obj) in PRIMITIVES:
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

class Channel(httplib.HTTPConnection):
  def __repr__(self):
    return 'HTTP(%s:%s)' % (self.host, self.port)
