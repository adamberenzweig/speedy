#!/usr/bin/env python

import logging
import rpc.client
import rpc.common
import rpc.server
import threading
import unittest

logging.basicConfig(format='%(asctime)s %(filename)s:%(funcName)s %(message)s',
                    level=logging.INFO)


class TestHandler(object):
  def test_echo(self, handle, v):
    handle.done(v)

  def test_exception(self, handle):
    raise Exception, 'Bob'

class RPCTestCase(unittest.TestCase):
  def setUp(self):
    self.port = rpc.common.find_open_port()
    mock = TestHandler()
    self._server = rpc.server.RPCServer(host='localhost', port=self.port, handler=mock)
    self._server.start()
    self.c = rpc.client.RPCClient('localhost', self.port)

  def tearDown(self):
    self._server.stop()

  def test_echo(self):
    self.assertEqual(self.c.test_echo('Hi!').wait(), 'Hi!')
    for i in range(10):
      self.assertEqual(self.c.test_echo(i).wait(), i)
  
  def test_exception(self):
    try:
      self.c.test_exception().wait()
    except Exception, e:
      logging.info('Exception caught! %s', e)
    else:
      assert False

  def test_crazy_string(self):
    s = ''.join([chr(i) for i in range(200)])
    self.assertEqual(self.c.test_echo(s).wait(), s)
    
  def test_many(self):
    handles = []
    for i in range(10000):
      if i % 1000 == 0: print i
      handles.append(self.c.test_echo('aaa'))
    
    for i, h in enumerate(handles):
      if i % 1000 == 0: print i
      h.wait()

  def test_connections(self):
    class ResultTester(threading.Thread):
      def __init__(self, client, v):
        threading.Thread.__init__(self)
        self.v = v
        self.client = client
        self.result = None
      
      def run(self):
        self.result = self.client.test_echo('Test%d' % self.v)
        
    threads = [ResultTester(self.c, i) for i in range(50)]
    [t.start() for t in threads]

    for i, t in enumerate(threads):
      t.join()
      assert t.result == 'Test%d' % i

if __name__ == '__main__':
  unittest.main()
#  import cProfile
#  cProfile.run('unittest.main()', 'profile.out')
