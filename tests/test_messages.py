from httprpc import proto, log
import unittest

def IsMessageClass(klass):
  try:
    return issubclass(klass, proto.Message) and not klass == proto.Message
  except TypeError:
    return False

MESSAGES = [
  v for v in proto.__dict__.values() if IsMessageClass(v)
]

class TestMessage(proto.Message):
  FIELDS = {
    'strval' : proto.Field.String(),
    'intval' : proto.Field.Int(),
  }


class MessageTestCase(unittest.TestCase):
  def test_serialization(self):
    log.info('Serializing proto...')
    for m in MESSAGES:
      log.info('PMessage: %s -- %s', m, proto.store(m()))

    log.info('Parsing proto...')
    for m in MESSAGES:
      log.info('PMessage: %s -- %s', m, proto.load(proto.store(m())))

  def test_hashing(self):
    d = {}
    d[TestMessage()] = 0
    assert len(d) == 1
    d[TestMessage()] = 0
    assert len(d) == 1

    assert TestMessage() in d
    d[TestMessage(intval = 1)] = 0
    assert len(d) == 2
    assert TestMessage() in d
    assert TestMessage(intval = 1) in d
    assert not TestMessage(intval = 2) in d

  def test_comparison(self):
    m1 = TestMessage()
    m2 = TestMessage(intval = 2)
    m3 = TestMessage(strval = 'abc')

    assert m1 != m2
    assert m2 != m3
    assert m1 != m3

    m1.intval = 2
    assert m1 == m2
    assert m1 != m3

if __name__ == '__main__':
  unittest.main()
