from eventlet.green import socket
import eventlet.debug
import logging

def dump_eventlet():
  logging.warn('Listeners:\n %s', eventlet.debug.format_hub_listeners())
  logging.warn('Timers:\n %s', eventlet.debug.format_hub_timers())

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
