#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from sys import platform as _platform

if _platform == 'linux':
    from ._socketcan import SocketCANMedia as SocketCANMedia
