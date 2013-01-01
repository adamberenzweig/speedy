#!/usr/bin/env python

from setuptools import setup, find_packages, Command
setup(
    name = "speedy",
    description="Fast, non-blocking JSON based RPC system.",
    version = "0.20",
    author="Russell Power",
    author_email="power@cs.nyu.edu",
    license="BSD",
    url="http://github.com/rjpower/speedy",
    package_dir = { '' : 'src' },
    packages = ['rpc'],
    install_requires = [],
    long_description='''
Speedy - A Fast RPC System for Python
=====================================

A fast non-blocking JSON based RPC library for Python.

Usage
-----

Server
''''''

::

    class MyHandler(object):
        def foo(self, handle, arg1, arg2):
            handle.done(do_something(arg1, arg2))

    import rpc.server
    s = rpc.server.RPCServer('localhost', 9999, handler=MyHandler())
    s.start()

Client
''''''

::

    import rpc.client
    c = rpc.client.RPCClient('localhost', 9999)
    future = c.foo('Some data', 'would go here')
    assert future.wait() == 'Expected result.'

Feedback
--------

Questions, comments: power@cs.nyu.edu
    ''',
    classifiers=['Development Status :: 3 - Alpha',
                 'Topic :: Software Development :: Libraries',
                 'Topic :: System :: Clustering',
                 'Topic :: System :: Distributed Computing',
                 'License :: OSI Approved :: BSD License',
                 'Intended Audience :: Developers',
                 'Intended Audience :: System Administrators',
                 'Operating System :: POSIX',
                 'Programming Language :: Python :: 2.5',
                 'Programming Language :: Python :: 2.6',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.0',
                 'Programming Language :: Python :: 3.1',
                 'Programming Language :: Python :: 3.2',
                 ],
)
