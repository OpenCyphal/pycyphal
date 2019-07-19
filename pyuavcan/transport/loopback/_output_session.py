#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio

import pyuavcan.transport


TransferRouter = typing.Callable[[pyuavcan.transport.Transfer], typing.Awaitable[None]]


class LoopbackFeedback(pyuavcan.transport.Feedback):
    def __init__(self, transfer_timestamp: pyuavcan.transport.Timestamp):
        self._transfer_timestamp = transfer_timestamp

    @property
    def original_transfer_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._transfer_timestamp

    @property
    def first_frame_transmission_timestamp(self) -> pyuavcan.transport.Timestamp:
        return self._transfer_timestamp


class LoopbackOutputSession(pyuavcan.transport.OutputSession):
    def __init__(self,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 loop:             asyncio.AbstractEventLoop,
                 closer:           typing.Callable[[], None],
                 router:           TransferRouter):
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        self._closer = closer
        self._router = router
        self._stats = pyuavcan.transport.Statistics()
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        await self._router(transfer)

        self._stats.transfers += 1
        self._stats.frames += 1
        self._stats.payload_bytes += sum(map(len, transfer.fragmented_payload))

        if self._feedback_handler is not None:
            self._feedback_handler(LoopbackFeedback(transfer.timestamp))

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        return self._stats

    def close(self) -> None:
        self._closer()


def _unittest_session() -> None:
    closed = False

    specifier = pyuavcan.transport.SessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 123)
    payload_metadata = pyuavcan.transport.PayloadMetadata(0xdeadbeef0ddf00d, 1234)

    def do_close() -> None:
        nonlocal closed
        closed = True

    async def do_route(_: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    ses = LoopbackOutputSession(specifier=specifier,
                                payload_metadata=payload_metadata,
                                loop=asyncio.get_event_loop(),
                                closer=do_close,
                                router=do_route)

    assert specifier == ses.specifier
    assert payload_metadata == ses.payload_metadata

    assert not closed
    ses.close()
    assert closed

    ts = pyuavcan.transport.Timestamp.now()
    fb = LoopbackFeedback(ts)
    assert fb.first_frame_transmission_timestamp == ts
    assert fb.original_transfer_timestamp == ts
