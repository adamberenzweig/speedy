from rpc import SocketError
from select import EPOLLIN, EPOLLOUT, EPOLLERR
import fcntl
import logging
import os
import select
import socket
import sys
import threading

class EPollWorker(object):
  '''
  Polls a set of active socket objects, reading and writing from them
  as data is available.
'''
  def __init__(self, name = 'EPollWorker'):
    self.name = name
    self._epoll = select.epoll(500)
    self._running = True
    self._lock = threading.Lock()
    self._pending = []
    self._sockets = {}

    self._wake_pipe_r, self._wake_pipe_w = os.pipe()
    fl = fcntl.fcntl(self._wake_pipe_r, fcntl.F_GETFL)
    fcntl.fcntl(self._wake_pipe_r, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    
    self._epoll.register(self._wake_pipe_r)
    self._thread = threading.Thread(name = name, target = self.run)
    self._thread.setDaemon(True)
    self._thread.start()

  def stop(self):
    logging.info('Shutting down PollWorker')
    self._running = False
    self._thread.join()
    for fd, _ in self._sockets.items():
      self.unregister(fd)

  def register(self, conn):
    with self._lock:
      logging.debug('Registering %s (%d)', self.name, conn.fileno())
      conn.poller = self
      self._pending.append(conn)

  def unregister(self, fd):
    with self._lock:
      logging.debug('Dropping %s (%d)', self.name, fd)
      self._epoll.unregister(fd)
      socket = self._sockets[fd]
      socket.poller = None
      socket.close()
      del self._sockets[fd]

  def wakeup(self):
    os.write(self._wake_pipe_w, '*')

  def run(self):
    while self._running:
      try:
        self._poll_loop()
      except IOError:
        logging.debug('Ignoring IO error during poll.')
      except Exception:
        logging.warn('Error during poll loop, continuing...', exc_info = 1)

    logging.info('Killing all client connections')
    [conn.close() for conn in self._sockets.values()]
    logging.info('Client poller shutdown succesfully.')

  def _poll_loop(self):
    while self._running:
#      logging.debug('Polling %d objects...', len(self._sockets))  
      events = self._epoll.poll(0.1)

      for fd, ev in events:
        try:
          if fd == self._wake_pipe_r:
            os.read(self._wake_pipe_r, 1000000)
            continue

          conn = self._sockets[fd]
          if ev & EPOLLIN:
#            logging.info('Reading...')
            conn.read_some_bytes()
          if ev & EPOLLOUT:
#            logging.info('Writing...')
            conn.write_some_bytes()
          if ev & EPOLLERR:
            raise SocketError, 'Socket error detected.'
        except socket.error:
          self.unregister(fd)

      # update epoll and add any pending sockets
      with self._lock:
        for conn in self._sockets.values():
          if conn.has_pending_writes():
            self._epoll.modify(conn, EPOLLIN | EPOLLOUT | EPOLLERR)
          else:
            self._epoll.modify(conn, EPOLLIN | EPOLLERR)

        for conn in self._pending:
            self._epoll.register(conn, EPOLLIN | EPOLLERR)
            self._sockets[conn.fileno()] = conn

        self._pending = []
      
