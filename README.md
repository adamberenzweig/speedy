Speedy - A Fast RPC System for Python
=====================================

A fast non-blocking RPC library for Python.

Installation
------------

    pip install [--user] speedy

or

    easy_install speedy

Usage
-----
##### Imports
    
    import speedy
    from speedy import zeromq
    
##### Server
    
    class MyServer(speedy.Server):
        def foo(self, handle, request):
            handle.done(do_something(request.foo, request.bar))
    server = MyServer(zeromq.server_socket(('127.0.0.1', port)))
    # or use -1 to have the server grab an open port
    # server = MyServer(zeromq.server_socket(('127.0.0.1', -1)))
    server.serve() # blocks until server exits

##### Client

    client = speedy.Client(zeromq.client_socket(('127.0.0.1', server_port)))
    
    # requests are arbitrary python objects
    request = { 'foo' : 123, 'bar' : 456 }
    
    future = client.foo(request)
    
    # Wait for the result.   If the server encountered an error,
    # an speedy.RemoteException will be thrown. 
    result = future.wait()

Feedback
--------

Questions, comments: <power@cs.nyu.edu>
