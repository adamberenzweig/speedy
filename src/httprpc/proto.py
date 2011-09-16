#!/usr/bin/env python

import pprint
import yaml

'''Simple typed message serialization library.

This module specifies a small embedded DSL for defining RPC messages.  Messages
are restricted Python objects.  All fields for a message must be declared 
ahead of time using the FIELDS dictionary, and all values are typechecked at
insertion time.

An example:

class AMessage(Message):
  FIELDS = {
    'a' : Field.Int(),
    'b' : Field.Int(),
    'c' : Field.String(),
    'd' : Field.Map(Field.String(), Field.Int())
    'e' : Field.Message(AnotherMessageClass)
  }
  
a = AMessage()
a.a = 1
a.b = 2
a.c = 'hello world'
a.d['3'] = 4
a.e = AnotherMessageClass()

bytes = store(a)
'''


def load(data):
  '''Takes as input a string and returns a deserialized output object'''
  return yaml.load(data)

def store(obj):
  '''Takes as input an object and returns a serialized string representation.'''
  return yaml.dump(obj)

class TypeInfo(object):
  def __init__(self, klass):
    self._klass = klass

  def __repr__(self):
    return 'TypeInfo(%s)' % self._klass.__name__

  def check_valid(self, obj):
    if not obj: return True
    if not isinstance(obj, self._klass):
      raise ValueError, 'Invalid value assignment %s; required type %s'\
        % (type(obj), self._klass.__name__)

class Field(object):
  def __init__(self, ftype, default_value_gen):
    self.type = ftype
    self.default_value = default_value_gen

  def __repr__(self):
    return 'Field(%s, default = "%s")' % (self.type, self.default_value())

  @staticmethod
  def Int(default_value = 0):
    return Field(TypeInfo(int), lambda: default_value)

  @staticmethod
  def Float(default_value = 0.):
    return Field(TypeInfo(float), lambda: default_value)

  @staticmethod
  def String(default_value = ''):
    return Field(TypeInfo(type('')), lambda: default_value)

  @staticmethod
  def Boolean(default_value = True):
    return Field(TypeInfo(bool), lambda: default_value)

  @staticmethod
  def Message(klass):
    return Field(TypeInfo(klass), lambda: klass())

  class _ListType(TypeInfo):
    def __init__(self, subtype):
      self._subtype = subtype

    def check_valid(self, obj):
      if not isinstance(obj, list): raise ValueError, 'Expected list type.'
      for v in obj:
        self._subtype.check_valid(v)

  @staticmethod
  def List(field):
    return Field(Field._ListType(field.type), lambda: [])

  class _MapType(TypeInfo):
    def __init__(self, ktype, vtype):
      self._ktype = ktype
      self._vtype = vtype

    def check_valid(self, obj):
      if not isinstance(obj, type({})): raise ValueError, 'Expected map.'
      for k, v in obj.iteritems():
        self._ktype.check_valid(k)
        self._vtype.check_valid(v)

  @staticmethod
  def Map(key, value):
    return Field(Field._MapType(key.type, value.type), lambda: {})


class Message(object):
  '''A message is a python object suitable for distribution over the network.

Each message subclass must specify a dictionary of zero or more fields.  A
field corresponds to an attribute which will be serialized / deserialized with
the message.
'''
  def __init__(self, **kw):
    for k, v in self.FIELDS.iteritems():
      assert isinstance(v, Field), 'Expected field instance (%s, %s)' % (k, v)
      setattr(self, k, v.default_value())

    for k, v in kw.iteritems():
      setattr(self, k, v)

  def __repr__(self):
    changed_items = dict([(k, getattr(self, k))
                          for k in self.FIELDS.keys() if
                          getattr(self, k) != self.FIELDS[k].default_value()])

    return '%s(%s)' % (
      self.__class__.__name__,
      pprint.pformat(changed_items))

  def __hash__(self):
    hashval = 0
    for k in self.FIELDS.iterkeys():
      hashval ^= hash(getattr(self, k))
    return hashval

  def __str__(self):
    return repr(self)

  def __eq__(self, other):
    if type(other) != type(self): return False
    return cmp(self, other) == 0

  def __cmp__(self, other):
    for k in self.FIELDS.keys():
      c = cmp(getattr(self, k), getattr(other, k))
      if c < 0: return -1
      if c > 0: return 1
    return 0

  def __setattr__(self, k, v):
    if k.startswith('_'):
      return object.__setattr__(self, k, v)

    if not k in self.FIELDS:
      raise KeyError, 'Attempting to set unknown field "%s" in message %s' % \
                       (k, self.__class__.__name__)

    self.FIELDS[k].type.check_valid(v)
    self.__dict__[k] = v

  def validate(self):
    for k in self.FIELDS.iterkeys():
      v = getattr(self, k)
      self.FIELDS[k].type.check_valid(v)

  def copy(self):
    return self.__class__(
     **dict((k, getattr(self, k)) for k in self.FIELDS.iterkeys()))


def make_doc(name, fields):
  return name + ':\n' + '\n'.join('%20s : %s' % (k, t)
                                  for (k, t) in fields.items())
