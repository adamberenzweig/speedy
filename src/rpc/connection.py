from rpc import RPCError, SocketError
import collections
import logging
import struct

class BufferedConnection(object):
  def __init__(self, socket, addr):
    self.host = addr[0]
    self.port = addr[1]
    self.poller = None
    self._socket = socket
    self._socket.setblocking(False)
    self._pending_writes = collections.deque()
    self._pending_reads = collections.deque()
    self._read_buffer = ''

  def __repr__(self):
    return repr(self._socket)

  def close(self):
    self._socket.close()

  def fileno(self):
    return self._socket.fileno()

  def read_finished(self):
    '''Invoked whenever any data has been read.'''
    raise NotImplementedError

  def write_finished(self):
    '''Invoked whenever any data has been written.'''
    raise NotImplementedError

  def has_pending_writes(self):
    return self._pending_writes

  def push_message(self, rpc_id, message):
    header = struct.pack('II', len(message), rpc_id)
    self._pending_writes.append((rpc_id, header + message))
    if self.poller:
      self.poller.wakeup()

  def pop_message(self):
    return self._pending_reads.popleft()

  def has_message(self):
    return self._pending_reads

  def read_some_bytes(self):
    in_bytes = self._socket.recv(100000)
    self._read_buffer = self._read_buffer + in_bytes

    # A read size of 0 is used to detect a connection close.
    if len(in_bytes) == 0:
      raise SocketError, 'Connection closed.'

    while len(self._read_buffer) > 8:
      (payload_len, rpcid) = struct.unpack('II', self._read_buffer[:8])
      if len(self._read_buffer) >= 8 + payload_len:
        payload = self._read_buffer[8:8 + payload_len]
        self._pending_reads.append((rpcid, payload))
        self._read_buffer = self._read_buffer[8 + payload_len:]
      else:
        break

#    logging.info('Read %d bytes', len(in_bytes))
    self.read_finished()
    return len(in_bytes)

  def write_some_bytes(self):
    if not self._pending_writes:
      return 0

    rpcid, write_buffer = self._pending_writes.popleft()
    bytes_written = self._socket.send(write_buffer)
    if bytes_written != len(write_buffer):
      self._pending_writes.appendleft((rpcid, write_buffer[bytes_written:]))
      self.write_finished()
    else:
      pass
#      logging.info('Finished send of rpc %s', rpcid)

#    logging.info('Wrote %d bytes', bytes_written)
    return bytes_written
