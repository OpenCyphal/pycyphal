#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import xml.etree.ElementTree
import pytest
import pyuavcan.transport
# Shouldn't import a transport from inside a coroutine because it triggers debug warnings.
from pyuavcan.transport.udp import UDPTransport, UDPTransportStatistics, UDPFrame


@pytest.mark.asyncio    # type: ignore
async def _unittest_serial_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    get_monotonic = asyncio.get_event_loop().time

    service_multiplication_factor = 2
