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
from pyuavcan.transport.redundant import RedundantTransport, RedundantTransportStatistics
from pyuavcan.transport.redundant import InconsistentInferiorConfigurationError
from pyuavcan.transport.loopback import LoopbackTransport
from pyuavcan.transport.serial import SerialTransport
from pyuavcan.transport.udp import UDPTransport


@pytest.mark.asyncio    # type: ignore
async def _unittest_redundant_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Timestamp, InputSessionSpecifier, OutputSessionSpecifier
    from pyuavcan.transport import ProtocolParameters

    tr = RedundantTransport()
    assert tr.sample_statistics() == RedundantTransportStatistics([])
    assert tr.inferiors == []
    assert tr.local_node_id is None
    assert tr.loop is asyncio.get_event_loop()
    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=0,
        max_nodes=0,
        mtu=0,
    )
