#!/usr/bin/env python

from distutils.core import setup

args = dict(
    name='uavcan',
    version='0.1',
    description='UAVCAN for Python',
    packages=['uavcan', 'uavcan.dsdl'],
    author='Pavel Kirienko',
    author_email='pavel.kirienko@gmail.com',
    url='http://uavcan.org',
    license='MIT'
)

setup(**args)
