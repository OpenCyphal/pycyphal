#!/usr/bin/env python
#
# Copyright (C) 2014-2020  UAVCAN Development Team  <uavcan.org>
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
VERSION_FILE = os.path.join(os.path.dirname(__file__), 'pyuavcan_v0', 'version.py')
exec(open(VERSION_FILE).read())         # Adds __version__ to globals

with open('README.md', 'r') as fh:
    long_description = fh.read()

args = dict(
    name='pyuavcan_v0',
    version=__version__,
    description='UAVCAN/CAN v0 (legacy) implementation in Python. Checkout pyuavcan for v1 implementation.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=[
        'pyuavcan_v0',
        'pyuavcan_v0.dsdl',
        'pyuavcan_v0.driver',
        'pyuavcan_v0.app',
    ],
    package_data={
        'pyuavcan_v0': [os.path.join(root[len('pyuavcan_v0/'):], fname)
                   for root, dirs, files in os.walk('pyuavcan_v0/dsdl_files')
                   for fname in files if fname.endswith('.uavcan')]
    },
    author='Pavel Kirienko, Ben Dyer',
    author_email='maintainers@uavcan.org',
    url='http://uavcan.org',
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
