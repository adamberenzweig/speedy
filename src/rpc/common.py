import socket
import traceback
import cPickle

class RemoteException(object):
  def __init__(self, exc_info):
    self.exception = exc_info[1]
    tb  = '\n'.join(traceback.format_exception(*exc_info))
    self.formatted_tb = tb.replace('\n', '\n>>')

class Message(object):
  '''A simple helper class to build POD type objects.
  
  Object attributes can be set via keyword arguments in the constructor.'''
  def __init__(self, **kw):
    for k, v in kw.iteritems():
      setattr(self, k, v)

  def __repr__(self):
    return '%s(\n%s)' % (
      self.__class__.__name__,
      ',\n'.join(['%s : %s' % (k, v)
                  for (k, v) in self.as_dict().items()]))

  def as_dict(self):
    return dict((k, v) for k, v in self.__dict__.iteritems() if k[0] != '_')

def find_open_port():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(("", 0))
  s.listen(1)
  port = s.getsockname()[1]
  s.close()
  return port

def split_addr(hostport):
  host, port = hostport.split(':')
  return host, int(port)


def pickle(v):
  return cPickle.dumps(v, -1)
  #return jsonpickle.encode(v)
  #return yaml.dump(v)


def unpickle(str):
  return cPickle.loads(str)
  #return jsonpickle.decode(str)
  #return yaml.load(str)
