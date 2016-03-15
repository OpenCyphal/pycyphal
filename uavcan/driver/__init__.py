#
# Copyright (C) 2014-2016  UAVCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
# Author: Ben Dyer <ben_dyer@mac.com>
#         Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import division, absolute_import, print_function, unicode_literals
import sys
from .slcan import SLCAN
from .common import DriverError, CANFrame

if sys.platform.startswith('linux'):
    from .socketcan import SocketCAN
else:
    SocketCAN = None

__all__ = ['make_driver', 'DriverError', 'CANFrame']


def make_driver(device_name, **kwargs):
    """Creates an instance of CAN driver.
    The right driver class will be selected automatically based on the device_name.
    :param device_name: This parameter is used to select driver class. E.g. "/dev/ttyACM0", "COM9", "can0".
    :param kwargs: Passed directly to the constructor.
    """
    windows_com_port = device_name.replace('\\', '').replace('.', '').lower().startswith('com')
    unix_tty = device_name.startswith('/dev/')

    if windows_com_port or unix_tty:
        return SLCAN(device_name, **kwargs)
    elif SocketCAN is not None:
        return SocketCAN(device_name, **kwargs)
    else:
        raise DriverError('Unrecognized device name: %r' % device_name)
