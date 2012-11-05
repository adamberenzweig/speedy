Speedy - A Fast RPC System for Python
=====================================

A fast non-blocking JSON based RPC library for Python.


Usage
-----

##### Server
    
    class MyHandler(object):
        def foo(self, handle, arg1, arg2):
            handle.done(do_something(arg1, arg2))

    import rpc.server
    s = rpc.server.RPCServer('localhost', 9999, handler=MyHandler())
    s.start()

##### Client

    import rpc.client
    c = rpc.client.RPCClient('localhost', 9999)
    future = c.foo('Some data', 'would go here')
    assert future.wait() == 'Expected result.'

Feedback
--------

Questions, comments: <power@cs.nyu.edu>
