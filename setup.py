#!/usr/bin/env python

from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages, Command
setup(
    name = "http-rpc",
    version = "0.1",
    package_dir = { '' : 'src' },
    packages = ['httprpc'],
    install_requires = [
      'distribute>=0.6.19',
      'eventlet>=0.9.16'
    ]
)
