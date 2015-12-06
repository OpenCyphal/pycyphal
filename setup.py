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

args = dict(
    name='uavcan',
    version='1.0.0dev3',
    description='Python implementation of the UAVCAN protocol stack',
    packages=['uavcan', 'uavcan.dsdl', 'uavcan.services', 'uavcan.monitors'],
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
