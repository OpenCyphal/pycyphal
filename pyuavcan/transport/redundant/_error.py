#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import pyuavcan.transport


class InconsistentInferiorConfigurationError(pyuavcan.transport.InvalidTransportConfigurationError):
    """
    Raised when a redundant transport instance is asked to attach a new inferior whose configuration
    does not match that of other inferiors.
    If there are no other inferiors, this error can never be raised.
    The following configuration parameters are validated:

    - The asyncio event loop instance the transport is running on shall be the same for all inferiors.
    - The local node-ID shall be the same for all inferiors (or all shall be anonymous).
    - The transfer-ID modulo shall meet EITHER of the following conditions:
        - Identical for all inferiors.
        - Not less than 2**48 for all inferiors.
    """
    pass
