#!/usr/bin/env python

from setuptools import setup, find_packages, Command, Extension
from Cython.Distutils import build_ext

setup(
    name="speedy",
    description="Fast, non-blocking RPC system.",
    version="0.22",
    author="Russell Power",
    author_email="power@cs.nyu.edu",
    license="BSD",
    url="http://github.com/rjpower/speedy",
    package_dir={ 'speedy' : 'speedy' },
    packages=['speedy'],
    install_requires=['pyzmq', 'numpy', 'cython'],
    long_description='''
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
    ''',

    cmdclass={'build_ext': build_ext},
    ext_modules=[
      Extension("speedy.core", ["speedy/core.pyx"])
    ]
)
