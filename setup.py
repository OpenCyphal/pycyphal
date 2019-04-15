#!/usr/bin/env python3
#
# Copyright (C) 2018  UAVCAN Development Team  <uavcan.org>
# This software is distributed under the terms of the MIT License.
#
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import sys
import setuptools

if int(setuptools.__version__.split('.')[0]) < 30:
    print('A newer version of setuptools is required. The current version does not support declarative config.',
          file=sys.stderr)
    sys.exit(1)

setuptools.setup()
