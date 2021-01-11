# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import pyuavcan.transport


class PortClosedError(pyuavcan.transport.ResourceClosedError):
    """
    Raised when an attempt is made to use a presentation-layer session instance that has been closed.
    Observe that it is a specialization of the corresponding transport-layer error type.
    Double-close is NOT an error, so closing the same instance twice will not result in this exception being raised.
    """


class RequestTransferIDVariabilityExhaustedError(pyuavcan.transport.TransportError):
    """
    Raised when an attempt is made to invoke more concurrent requests that supported by the transport layer.
    For CAN, the number is 32; for some transports the number is unlimited (technically, there is always a limit,
    but for some transports, such as the serial transport, it is unreachable in practice).
    """
