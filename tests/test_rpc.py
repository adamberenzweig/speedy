#!/usr/bin/env python

from eventlet import greenpool
from httprpc import proto, log
import httprpc.common
import httprpc.client
import httprpc.server
import unittest

class TestMessage(proto.Message):
  FIELDS = {
    'str' : proto.Field.String(),
    'int' : proto.Field.Int(),
  }

class MockServer(httprpc.server.Server):
  METHODS = {
    'test' : (TestMessage, TestMessage)
  }
  def test(self, req):
    log.info('Server called: %s', req)
    return req

class RPCTestCase(unittest.TestCase):
  def setUp(self):
    self.port = httprpc.common.find_open_port()
    self._server = MockServer(host = 'localhost', port = self.port)
    self._server.start()

    self.c = httprpc.client.Client('localhost', self.port)

  def tearDown(self):
    self._server.stop()

  def test_simple_server(self):
    req = TestMessage(str = 'Hi there!')
    log.info('Response: %s', self.c.test(req))

  def test_connections(self):
    gp = greenpool.GreenPool(size = 1000)

    threads = [gp.spawn(self.c.test,
                        TestMessage(str = 'Testing %d' % i,
                                     int = i)) for i in range(50)]

    for i, t in enumerate(threads):
      assert t.wait().int == i

  def test_local(self):
    gp = greenpool.GreenPool(size = 1000)

    threads = [gp.spawn(self._server.test,
                        TestMessage(str = 'Testing %d' % i,
                                     int = i)) for i in range(500)]

    for i, t in enumerate(threads):
      assert t.wait().int == i

if __name__ == '__main__':
  httprpc.common.enable_debugging()
  unittest.main()
