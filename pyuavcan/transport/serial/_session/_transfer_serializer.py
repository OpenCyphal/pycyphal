#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import itertools
import pyuavcan
from .._frame import Frame


def serialize_transfer(priority:                pyuavcan.transport.Priority,
                       local_node_id:           typing.Optional[int],
                       session_specifier:       pyuavcan.transport.SessionSpecifier,
                       data_type_hash:          int,
                       transfer_id:             int,
                       fragmented_payload:      typing.Sequence[memoryview],
                       max_frame_payload_bytes: int) -> typing.Iterable[Frame]:
    assert max_frame_payload_bytes > 0

    if local_node_id is None and isinstance(session_specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier):
        raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
            f'Anonymous nodes cannot emit service transfers. Session specifier: {session_specifier}')

    payload_length = sum(map(len, fragmented_payload))

    if payload_length <= max_frame_payload_bytes:               # SINGLE-FRAME TRANSFER
        payload = fragmented_payload[0] if len(fragmented_payload) == 0 else memoryview(b''.join(fragmented_payload))
        assert len(payload) == payload_length
        assert max_frame_payload_bytes >= len(payload)
        yield Frame(priority=priority,
                    source_node_id=local_node_id,
                    destination_node_id=session_specifier.remote_node_id,
                    data_specifier=session_specifier.data_specifier,
                    data_type_hash=data_type_hash,
                    transfer_id=transfer_id,
                    frame_index=0,
                    end_of_transfer=True,
                    payload=payload)
    else:                                                       # MULTI-FRAME TRANSFER
        if local_node_id is None:
            raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                f'Anonymous nodes cannot emit multi-frame transfers. Session specifier: {session_specifier}')

        # Serial transport uses the same CRC algorithm both for frames and transfers.
        crc = pyuavcan.transport.commons.crc.CRC32C()
        for frag in fragmented_payload:
            crc.add(frag)

        refragmented = pyuavcan.transport.commons.refragment(
            itertools.chain(fragmented_payload, (memoryview(crc.value_as_bytes),)),
            max_frame_payload_bytes
        )
        for frame_index, (end_of_transfer, frag) in enumerate(pyuavcan.util.mark_last(refragmented)):
            yield Frame(priority=priority,
                        source_node_id=local_node_id,
                        destination_node_id=session_specifier.remote_node_id,
                        data_specifier=session_specifier.data_specifier,
                        data_type_hash=data_type_hash,
                        transfer_id=transfer_id,
                        frame_index=frame_index,
                        end_of_transfer=end_of_transfer,
                        payload=frag)


def _unittest_serialize_transfer() -> None:
    from pytest import raises
    from pyuavcan.transport import Priority, SessionSpecifier, MessageDataSpecifier, ServiceDataSpecifier

    assert [
        Frame(
            priority=Priority.OPTIONAL,
            source_node_id=1234,
            destination_node_id=None,
            data_specifier=MessageDataSpecifier(4321),
            data_type_hash=0xdead_beef_0dd_c0ffe,
            transfer_id=12345678901234567890,
            frame_index=0,
            end_of_transfer=True,
            payload=memoryview(b'hello world'),
        ),
    ] == list(serialize_transfer(
        priority=Priority.OPTIONAL,
        local_node_id=1234,
        session_specifier=SessionSpecifier(MessageDataSpecifier(4321), None),
        data_type_hash=0xdead_beef_0dd_c0ffe,
        transfer_id=12345678901234567890,
        fragmented_payload=[memoryview(b'hello'), memoryview(b' '), memoryview(b'world')],
        max_frame_payload_bytes=100,
    ))

    with raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        _ = list(serialize_transfer(
            priority=Priority.SLOW,
            local_node_id=None,
            session_specifier=SessionSpecifier(MessageDataSpecifier(4321), None),
            data_type_hash=0xdead_beef_0dd_c0ffe,
            transfer_id=12345678901234567890,
            fragmented_payload=[memoryview(b'hello'), memoryview(b' '), memoryview(b'world')],
            max_frame_payload_bytes=5,
        ))

    with raises(pyuavcan.transport.OperationNotDefinedForAnonymousNodeError):
        _ = list(serialize_transfer(
            priority=Priority.SLOW,
            local_node_id=None,
            session_specifier=SessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
            data_type_hash=0xdead_beef_0dd_c0ffe,
            transfer_id=12345678901234567890,
            fragmented_payload=[memoryview(b'hello'), memoryview(b' '), memoryview(b'world')],
            max_frame_payload_bytes=1000,
        ))

    hello_world_crc = pyuavcan.transport.commons.crc.CRC32C()
    hello_world_crc.add(b'hello world')

    assert [
        Frame(
            priority=Priority.SLOW,
            source_node_id=1234,
            destination_node_id=2222,
            data_specifier=ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST),
            data_type_hash=0xdead_beef_0dd_c0ffe,
            transfer_id=12345678901234567890,
            frame_index=0,
            end_of_transfer=False,
            payload=memoryview(b'hello'),
        ),
        Frame(
            priority=Priority.SLOW,
            source_node_id=1234,
            destination_node_id=2222,
            data_specifier=ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST),
            data_type_hash=0xdead_beef_0dd_c0ffe,
            transfer_id=12345678901234567890,
            frame_index=1,
            end_of_transfer=False,
            payload=memoryview(b' worl'),
        ),
        Frame(
            priority=Priority.SLOW,
            source_node_id=1234,
            destination_node_id=2222,
            data_specifier=ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST),
            data_type_hash=0xdead_beef_0dd_c0ffe,
            transfer_id=12345678901234567890,
            frame_index=2,
            end_of_transfer=True,
            payload=memoryview(b'd' + hello_world_crc.value_as_bytes),
        ),
    ] == list(serialize_transfer(
        priority=Priority.SLOW,
        local_node_id=1234,
        session_specifier=SessionSpecifier(ServiceDataSpecifier(321, ServiceDataSpecifier.Role.REQUEST), 2222),
        data_type_hash=0xdead_beef_0dd_c0ffe,
        transfer_id=12345678901234567890,
        fragmented_payload=[memoryview(b'hello'), memoryview(b' '), memoryview(b'world')],
        max_frame_payload_bytes=5,
    ))
