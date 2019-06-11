#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pyuavcan.transport


class TypedSessionClosedError(pyuavcan.transport.ResourceClosedError):
    """
    Raised when an attempt is made to use a typed session instance that has been closed. Observe that it is a
    specialization of the corresponding transport-layer error type.
    """
    pass
