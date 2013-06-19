#!/usr/bin/env python

'''MPI "sockets"'''

from mpi4py import MPI
from sparrow.rpc.common import Group
import collections

import numpy as N
import threading
import time

SOCKET_ID = iter(xrange(1, 10000))

class MPIPoller(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self, target=self._run)
    self.setDaemon(True)
    self._sockets = {}
    self._running = True
    self._stopped = False

  def stop(self):
    self._running = False
    while not self._stopped:
      time.sleep(0)


  def add(self, socket):
    self._sockets[socket] = 1

  def remove(self, socket):
    del self._sockets[socket]

  def _run(self):
    while self._running:
      # util.log('Polling %s sockets', len(self._sockets))
      for v in self._sockets.keys():
        v.poll()
      time.sleep(0.001)
    self._stopped = True


POLLER = None
def init():
  global POLLER
  if POLLER is not None:
    return
  util.log('Creating polller...')
  POLLER = MPIPoller()
  POLLER.start()

def shutdown():
  POLLER.stop()

class Socket(object):
  def __init__(self, comm, dst_rank, dst_tag, src_tag=None, connected=False):
    self.comm = comm
    self.dst_rank = dst_rank
    self.dst_tag = dst_tag

    self._connecting = False
    self._connected = connected

    if src_tag is None:
      src_tag = SOCKET_ID.next()
    self.src_tag = src_tag

    self._out = collections.deque()
    self._pending = {}
    self._callback = None


  def close(self):
    POLLER.remove(self)

  def connect(self):
    if not self._connected:
      self._connecting = True
    POLLER.add(self)

  def send(self, *obj):
    self._out.extend(obj)

  def recv(self):
    count = self.comm.recv(source=self.dst_rank, tag=self.src_tag)
    res = list()
    for i in range(count):
      stat = MPI.Status()
      self.comm.Probe(source=self.dst_rank, tag=self.src_tag, status=stat)
      count = stat.Get_count(MPI.BYTE)
      buf = N.ndarray(count, dtype=N.uint8)
      self.comm.Recv(buf, source=self.dst_rank, tag=self.src_tag)
      res.append(str(N.getbuffer(buf)))
    return res

  def _send(self, obj):
    # util.log('Sent %s:%s, %s', self.dst_rank, self.dst_tag, obj)
    if isinstance(obj, Group):
      self.comm.bsend(len(obj), dest=self.dst_rank, tag=self.dst_tag)
      for i in obj:
        req = self.comm.Isend(i, dest=self.dst_rank, tag=self.dst_tag)
        self._pending[id(req)] = req
    else:
      self.comm.send(obj, dest=self.dst_rank, tag=self.dst_tag)

  def _continue_connect(self):
    # the new socket is going to try to connect to us now, send
    # back our tag again.
    if not self.comm.Iprobe(source=self.dst_rank, tag=self.src_tag):
      return

    new_tag = self.comm.recv(source=self.dst_rank, tag=self.src_tag)
    util.log('Got tag... %s', new_tag)
    self.dst_tag = new_tag

    self._connected = True
    self._connecting = False
    self._callback = None

  def poll(self):
    if self._callback is not None:
      self._callback()
      return

    if self._connecting:
      util.log('Connecting... %s, %s', self.dst_rank, self.dst_tag)
      self.comm.send(self.src_tag, dest=self.dst_rank, tag=self.dst_tag)
      self._callback = self._continue_connect
      return

    assert self._connected

    while self._out:
      obj = self._out.popleft()
      self._send(obj)

    for k, v in self._pending.items():
      if v.Test():
        del self._pending[k]

    # util.log('Testing for reads.... (%s, %d)', self.dst_rank, self.src_tag)
    status = MPI.Status()
    while self.comm.Iprobe(source=self.dst_rank, tag=self.src_tag, status=status):
      # util.log('Reading! %s %s', self.dst_rank, self.src_tag)
      self.handle_read(self)

class ServerSocket(object):
  def __init__(self, comms, tag= -1):
    self.comms = comms
    self.tag = tag


  def bind(self):
    if self.tag == -1:
      self.tag = SOCKET_ID.next()

    self.addr = (self.tag,)
    POLLER.add(self)

  def close(self):
    POLLER.remove(self)

  def poll(self):
    """Listen for incoming connections to (rank, tag).

    When connected, create a new socket to handle further communication.
    """
    status = MPI.Status()
    for comm in self.comms:
      while comm.Iprobe(tag=self.tag, status=status):
        rank, tag = status.Get_source(), status.Get_tag()
        util.log('Got request from %s %s; creating reverse socket', rank, tag)
        dst_tag = comm.recv(source=rank, tag=tag)

        # backwards socket to the client
        sock = Socket(comm, rank, dst_tag, connected=True)
        sock.handle_read = self.handle_read
        sock.connect()
        sock.send(sock.src_tag)

def client_socket(addr):
  comm, rank, tag = addr
  return Socket(comm, rank, tag)

def server_socket(addr):
  comms, tag = addr
  return ServerSocket(comms, tag)
