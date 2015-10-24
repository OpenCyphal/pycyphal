#!/usr/bin/env python

import os
from setuptools import setup

args = dict(
    name='uavcan',
    version='1.0dev1',
    description='UAVCAN for Python',
    packages=['uavcan', 'uavcan.dsdl', 'uavcan.services', 'uavcan.monitors'],
    package_data={
        'uavcan': [os.path.join(root[len('uavcan/'):], fname)
                   for root, dirs, files in os.walk('uavcan/dsdl_files')
                   for fname in files if fname.endswith('.uavcan')]
    },
    author='Pavel Kirienko',
    author_email='pavel.kirienko@gmail.com',
    url='https://github.com/UAVCAN/pyuavcan',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    keywords=''
)

setup(**args)

