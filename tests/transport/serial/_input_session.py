# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import asyncio
import typing
import pytest
from pytest import raises, approx
from pyuavcan.transport import InputSessionSpecifier, MessageDataSpecifier, Priority, TransferFrom
from pyuavcan.transport import PayloadMetadata, Timestamp
from pyuavcan.transport.commons.high_overhead_transport import TransferCRC
from pyuavcan.transport.serial._session._input import SerialInputSession
from pyuavcan.transport.serial import SerialFrame, SerialInputSessionStatistics
from pyuavcan.transport.commons.high_overhead_transport import TransferReassembler

pytestmark = pytest.mark.asyncio


async def _unittest_input_session() -> None:
    ts = Timestamp.now()
    prio = Priority.SLOW
    dst_nid = 1234

    get_monotonic = asyncio.get_event_loop().time

    nihil_supernum = b"nihil supernum"

    finalized = False

    def do_finalize() -> None:
        nonlocal finalized
        finalized = True

    session_spec = InputSessionSpecifier(MessageDataSpecifier(2345), None)
    payload_meta = PayloadMetadata(100)

    sis = SerialInputSession(specifier=session_spec, payload_metadata=payload_meta, finalizer=do_finalize)
    assert sis.specifier == session_spec
    assert sis.payload_metadata == payload_meta
    assert sis.sample_statistics() == SerialInputSessionStatistics()

    assert sis.transfer_id_timeout == approx(SerialInputSession.DEFAULT_TRANSFER_ID_TIMEOUT)
    sis.transfer_id_timeout = 1.0
    with raises(ValueError):
        sis.transfer_id_timeout = 0.0
    assert sis.transfer_id_timeout == approx(1.0)

    assert await (sis.receive(get_monotonic() + 0.1)) is None
    assert await (sis.receive(0.0)) is None

    def mk_frame(
        transfer_id: int,
        index: int,
        end_of_transfer: bool,
        payload: typing.Union[bytes, memoryview],
        source_node_id: typing.Optional[int],
    ) -> SerialFrame:
        return SerialFrame(
            priority=prio,
            transfer_id=transfer_id,
            index=index,
            end_of_transfer=end_of_transfer,
            payload=memoryview(payload),
            source_node_id=source_node_id,
            destination_node_id=dst_nid,
            data_specifier=session_spec.data_specifier,
        )

    # ANONYMOUS TRANSFERS.
    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=False, payload=nihil_supernum, source_node_id=None)
    )
    assert sis.sample_statistics() == SerialInputSessionStatistics(
        frames=1,
        errors=1,
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=1, end_of_transfer=True, payload=nihil_supernum, source_node_id=None)
    )
    assert sis.sample_statistics() == SerialInputSessionStatistics(
        frames=2,
        errors=2,
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=True, payload=nihil_supernum, source_node_id=None)
    )
    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=1,
        frames=3,
        payload_bytes=len(nihil_supernum),
        errors=2,
    )
    assert await (sis.receive(0)) == TransferFrom(
        timestamp=ts, priority=prio, transfer_id=0, fragmented_payload=[memoryview(nihil_supernum)], source_node_id=None
    )
    assert await (sis.receive(get_monotonic() + 0.1)) is None
    assert await (sis.receive(0.0)) is None

    # VALID TRANSFERS. Notice that they are unordered on purpose. The reassembler can deal with that.
    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=1, end_of_transfer=False, payload=nihil_supernum, source_node_id=1111)
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=True, payload=nihil_supernum, source_node_id=2222)
    )  # COMPLETED FIRST

    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=2,
        frames=5,
        payload_bytes=len(nihil_supernum) * 2,
        errors=2,
        reassembly_errors_per_source_node_id={
            1111: {},
            2222: {},
        },
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts,
        mk_frame(
            transfer_id=0,
            index=3,
            end_of_transfer=True,
            payload=TransferCRC.new(nihil_supernum * 3).value_as_bytes,
            source_node_id=1111,
        ),
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=0, end_of_transfer=False, payload=nihil_supernum, source_node_id=1111)
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts, mk_frame(transfer_id=0, index=2, end_of_transfer=False, payload=nihil_supernum, source_node_id=1111)
    )  # COMPLETED SECOND

    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=3,
        frames=8,
        payload_bytes=len(nihil_supernum) * 5,
        errors=2,
        reassembly_errors_per_source_node_id={
            1111: {},
            2222: {},
        },
    )

    assert await (sis.receive(0)) == TransferFrom(
        timestamp=ts, priority=prio, transfer_id=0, fragmented_payload=[memoryview(nihil_supernum)], source_node_id=2222
    )
    assert await (sis.receive(0)) == TransferFrom(
        timestamp=ts,
        priority=prio,
        transfer_id=0,
        fragmented_payload=[memoryview(nihil_supernum)] * 3,
        source_node_id=1111,
    )
    assert await (sis.receive(get_monotonic() + 0.1)) is None
    assert await (sis.receive(0.0)) is None

    # TRANSFERS WITH REASSEMBLY ERRORS.
    sis._process_frame(  # pylint: disable=protected-access
        ts,
        mk_frame(
            transfer_id=1, index=0, end_of_transfer=False, payload=b"", source_node_id=1111  # EMPTY IN MULTIFRAME
        ),
    )

    sis._process_frame(  # pylint: disable=protected-access
        ts,
        mk_frame(
            transfer_id=2, index=0, end_of_transfer=False, payload=b"", source_node_id=1111  # EMPTY IN MULTIFRAME
        ),
    )

    assert sis.sample_statistics() == SerialInputSessionStatistics(
        transfers=3,
        frames=10,
        payload_bytes=len(nihil_supernum) * 5,
        errors=4,
        reassembly_errors_per_source_node_id={
            1111: {
                TransferReassembler.Error.MULTIFRAME_EMPTY_FRAME: 2,
            },
            2222: {},
        },
    )

    assert not finalized
    sis.close()
    assert finalized
    sis.close()  # Idempotency check
