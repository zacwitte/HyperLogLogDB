#!/usr/bin/env python

from distutils.core import setup

version = '0.1.0'

setup(
    name='hyperloglogdb',
    version=version,
    author='Zac Witte',
    author_email='zacwitte@gmail.com',
    maintainer='Zac Witte',
    maintainer_email='zacwitte@gmail.com',
    packages=['hyperloglogdb', 'hyperloglogdb.test'],
    url='https://github.com/zacwitte/HyperLogLogDB',
    license='LGPL',
    description='A disk-based database of HyperLogLog data structures for estimating cardinality of many distinct sets',
    long_description=open('README.md').read(),
    install_requires=['numpy']
)
