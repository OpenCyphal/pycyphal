# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import pyuavcan.transport


class InconsistentInferiorConfigurationError(pyuavcan.transport.InvalidTransportConfigurationError):
    """
    Raised when a redundant transport instance is asked to attach a new inferior whose configuration
    does not match that of the other inferiors or of the redundant transport itself.
    """
