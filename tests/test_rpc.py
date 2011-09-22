#!/usr/bin/env python

from eventlet import greenpool
import httprpc.client
import httprpc.common
import httprpc.server
import logging
import unittest

class MockObject(object):
  def test_echo(self, v):
    return v

  def test_inner(self, v):
    class Inner(object):
      def foo(self):
        return v

      def bar(self):
        return 2 * v

    return Inner()

  def test_exception(self):
    raise Exception, 'Bob'

class RPCTestCase(unittest.TestCase):
  def setUp(self):
    self.port = httprpc.common.find_open_port()
    self._server = httprpc.server.Server(host = 'localhost', port = self.port)
    self._server.register_object('mock', MockObject())
    self._server.start()

    self.c = httprpc.client.Client('localhost', self.port)

  def tearDown(self):
    self._server.stop()

  def test_echo(self):
    self.assertEqual(self.c.mock.test_echo('Hi!'), 'Hi!')
    for i in range(10):
      self.assertEqual(self.c.mock.test_echo(i), i)

  def test_rmi(self):
    inner = self.c.mock.test_inner(10)
    self.assert_(inner)
    self.assertEqual(inner.foo(), 10)
    self.assertEqual(inner.bar(), 20)

  def test_exception(self):
    try:
      self.c.mock.test_exception()
    except Exception, e:
      logging.info('Exception caught! %s', e)
    else:
      assert False

  def test_connections(self):
    gp = greenpool.GreenPool(size = 1000)

    threads = [gp.spawn(self.c.mock.test_echo, 'Test%d' % i) for i in range(50)]

    for i, t in enumerate(threads):
      assert t.wait() == 'Test%d' % i


if __name__ == '__main__':
  httprpc.common.enable_debugging()
  unittest.main()
