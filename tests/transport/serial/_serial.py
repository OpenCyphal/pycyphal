#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import asyncio
import xml.etree.ElementTree
import pytest
import serial
import pyuavcan.transport


@pytest.mark.asyncio    # type: ignore
async def _unittest_can_transport() -> None:
    from pyuavcan.transport import MessageDataSpecifier, ServiceDataSpecifier, PayloadMetadata, Transfer, TransferFrom
    from pyuavcan.transport import Priority, Statistics, Timestamp, SessionSpecifier, ProtocolParameters
    from pyuavcan.transport.serial import SerialTransport, SerialStatistics

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port='loop://',
                            single_frame_transfer_payload_capacity_bytes=1)

    with pytest.raises(ValueError):
        _ = SerialTransport(serial_port='loop://',
                            service_transfer_multiplier=10000)

    with pytest.raises(pyuavcan.transport.InvalidMediaConfigurationError):
        _ = SerialTransport(serial_port=serial.serial_for_url('loop://', do_not_open=True))

    tr = SerialTransport(serial_port='loop://')

    assert tr.loop is asyncio.get_event_loop()
    assert tr.local_node_id is None
    assert tr.serial_port.is_open

    assert tr.input_sessions == []
    assert tr.output_sessions == []

    assert list(xml.etree.ElementTree.fromstring(tr.descriptor).itertext()) == ['loop://']
    assert str(SerialTransport.DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES) in tr.descriptor

    assert tr.protocol_parameters == ProtocolParameters(
        transfer_id_modulo=2 ** 64,
        node_id_set_cardinality=4096,
        single_frame_transfer_payload_capacity_bytes=SerialTransport
        .DEFAULT_SINGLE_FRAME_TRANSFER_PAYLOAD_CAPACITY_BYTES,
    )

    assert tr.sample_statistics() == SerialStatistics()
