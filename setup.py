#!/usr/bin/env python
#
# Copyright (C) 2014-2015  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

import os
import sys
from setuptools import setup

__version__ = None
VERSION_FILE = os.path.join(os.path.dirname(__file__), 'uavcan', 'version.py')
exec(open(VERSION_FILE).read())         # Adds __version__ to globals


args = dict(
    name='uavcan',
    version=__version__,
    description='Python implementation of the UAVCAN protocol stack',
    packages=[
        'uavcan',
        'uavcan.dsdl',
        'uavcan.driver',
        'uavcan.app',
    ],
    package_data={
        'uavcan': [os.path.join(root[len('uavcan/'):], fname)
                   for root, dirs, files in os.walk('uavcan/dsdl_files')
                   for fname in files if fname.endswith('.uavcan')]
    },
    author='Pavel Kirienko, Ben Dyer',
    author_email='uavcan@googlegroups.com',
    url='http://uavcan.org/Implementations/Pyuavcan',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    ],
    keywords=''
)

if sys.version_info[0] < 3:
    args['install_requires'] = ['monotonic']

setup(**args)
