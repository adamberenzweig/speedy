#!/usr/bin/env python

import threading
import unittest

import numpy as N
import speedy
from speedy import zeromq, wait_for_all, util
from numpy.random import randn

class Empty(object):
  pass

class Ping(object):
  def __init__(self, ping):
    self.ping = ping


class Pong(object):
  def __init__(self, pong):
    self.pong = pong


class EchoWorker(speedy.Server):
  def __init__(self, *args, **kw):
    speedy.Server.__init__(self, *args, **kw)

  def ping(self, handle, req):
    handle.done(Pong(pong=req.ping))

  def thread_ping(self, handle, req):
    t = threading.Thread(target=lambda: handle.done(Pong(pong=req.ping)))
    t.start()

  def ping_no_reply(self, handle, req):
    handle.done()

class PingWorker(threading.Thread):
  def __init__(self, addr):
    threading.Thread.__init__(self)
    self.client = speedy.Client(zeromq.client_socket(addr))

  def run(self):
    content = randn(100)
    req = Ping(ping=content)
    futures = []
    for x in range(100):
      futures.append(self.client.ping(req))

    results = wait_for_all(futures)
    for r in results : assert N.all(r.pong == content)
    self.client.close()

class RPCTest(unittest.TestCase):
  def setUp(self):
    self.server = EchoWorker(zeromq.server_socket(('127.0.0.1', -1)))
    self.server.serve_nonblock()

  def tearDown(self):
    self.server.shutdown()

  def _connect(self):
    return speedy.Client(zeromq.client_socket(self.server.addr))

  def test_sendrecv(self):
    with self._connect() as proxy:
      ping_req = proxy.ping(Ping(ping='Hello!'))
      res = ping_req.wait()
      self.assertEqual(res.pong, 'Hello!')

  def test_latency(self):
    with self._connect() as proxy:
      ping_req = Ping(ping='Hello!')
      with util.timer_ctx('send 100 requests'):
        for i in range(100):
          proxy.ping(ping_req).wait()

  def test_perf(self):
    with self._connect() as proxy:
      big_str = N.ndarray(1000 * 1000 * 10, dtype=N.uint32)

      req = Ping(ping=big_str)
      with util.timer_ctx('send 400MB'):
        for i in range(10):
          ping_req = proxy.ping(req)
          ping_req.wait()
  #      self.assertEqual(ping_req.wait(), Pong(pong=big_str))

  def test_threads(self):
    with self._connect() as proxy:
      ping_req = Ping(ping='Hello!')
      futures = []
      for i in range(100):
        futures.append(proxy.thread_ping(ping_req))
      for i in range(100):
        futures[i].wait()

  def test_ping_no_reply(self):
    with self._connect() as proxy:
      ping_req = Ping(ping=N.ndarray(1000, dtype=N.float))
      futures = []
      for i in range(100):
        futures.append(proxy.ping_no_reply(ping_req))
      for i in range(100):
        futures[i].wait()

  def test_concurrent_send(self):
    workers = [PingWorker(self.server.addr) for x in range(50)]
    for w in workers: w.start()
    for w in workers: w.join()

  def test_reponse(self):
    with self._connect() as proxy:
      content = "hello"
      req = Ping(ping=content)
      futures = []
      test_num = 1000
      for x in range(test_num):
        futures.append(proxy.ping(req))

      results = wait_for_all(futures)
      none_num = 0
      for r in results:
        if r is None: none_num += 1

      self.assertEqual(none_num, 0)

import unittest
unittest.main()
