#!/usr/bin/env python

import os.path
README = open(os.path.abspath(os.path.dirname(__file__)) + '/README.md').read()

from setuptools import setup, find_packages, Command
setup(
    name = "speedy",
    description="Fast, non-blocking JSON based RPC system.",
    version = "0.12",
    author="Russell Power",
    author_email="power@cs.nyu.edu",
    license="BSD",
    url="http://github.com/rjpower/speedy",
    data_files = [('.', ['README.md'])],
    package_dir = { '' : 'src' },
    packages = ['rpc'],
    requires = [
      'PyYAML',
      'jsonpickle',
    ],
    install_requires = [ 
      'PyYAML',  
      'jsonpickle',
    ],
    long_description=README,
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
