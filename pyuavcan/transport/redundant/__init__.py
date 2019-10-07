#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
Redundant pseudo-transport overview
+++++++++++++++++++++++++++++++++++



Usage
+++++



Inheritance diagram
+++++++++++++++++++

.. inheritance-diagram:: pyuavcan.transport.redundant._redundant_transport
                         pyuavcan.transport.redundant._error
                         pyuavcan.transport.redundant._session._base
                         pyuavcan.transport.redundant._session._input
                         pyuavcan.transport.redundant._session._output
                         pyuavcan.transport.redundant._deduplicator._base
                         pyuavcan.transport.redundant._deduplicator._monotonic
                         pyuavcan.transport.redundant._deduplicator._cyclic
   :parts: 1
"""

from ._redundant_transport import RedundantTransport as RedundantTransport
from ._redundant_transport import RedundantTransportStatistics as RedundantTransportStatistics

from ._session import RedundantSession as RedundantSession
from ._session import RedundantInputSession as RedundantInputSession
from ._session import RedundantOutputSession as RedundantOutputSession

from ._session import RedundantSessionStatistics as RedundantSessionStatistics
from ._session import RedundantFeedback as RedundantFeedback

from ._error import InconsistentInferiorConfigurationError as InconsistentInferiorConfigurationError
