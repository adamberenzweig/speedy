from eventlet.green import socket
from httprpc.proto import Message, Field
from httprpc import log
import eventlet.debug

class RPCHeader(Message):
  FIELDS = {
    'id' : Field.Int(),
    'method' : Field.String(),
    'request_size' : Field.Int(),
  }

class ServerErrorResponse(Message):
  FIELDS = {
    'exception' : Field.String(),
    'message' : Field.String(),
    'traceback' : Field.String()
  }

class RPCError(Exception):
  pass

class ConnectionLost(RPCError):
  pass

class ServerError(RPCError):
  pass


def dump_eventlet():
  log.warn('Listeners:\n %s', eventlet.debug.format_hub_listeners())
  log.warn('Timers:\n %s', eventlet.debug.format_hub_timers())

def enable_debugging():
  eventlet.debug.hub_listener_stacks(True)
  eventlet.debug.hub_timer_stacks(True)

  import atexit
  atexit.register(dump_eventlet)

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
