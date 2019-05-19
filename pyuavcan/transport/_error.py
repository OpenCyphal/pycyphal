#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#


class TransportError(RuntimeError):
    pass


class UnsupportedSessionConfigurationError(TransportError):
    pass


class OperationNotDefinedForAnonymousNodeError(TransportError):
    pass
