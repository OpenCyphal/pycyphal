# Copyright (c) 2019 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

import pycyphal.transport


class InconsistentInferiorConfigurationError(pycyphal.transport.InvalidTransportConfigurationError):
    """
    Raised when a redundant transport instance is asked to attach a new inferior whose configuration
    does not match that of the other inferiors or of the redundant transport itself.
    """
