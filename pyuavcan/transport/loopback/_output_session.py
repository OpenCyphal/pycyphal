# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio

import pyuavcan.transport


TransferRouter = typing.Callable[[pyuavcan.transport.Transfer, float], typing.Awaitable[bool]]


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
    def __init__(
        self,
        specifier: pyuavcan.transport.OutputSessionSpecifier,
        payload_metadata: pyuavcan.transport.PayloadMetadata,
        loop: asyncio.AbstractEventLoop,
        closer: typing.Callable[[], None],
        router: TransferRouter,
    ):
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        self._closer = closer
        self._router = router
        self._stats = pyuavcan.transport.SessionStatistics()
        self._feedback_handler: typing.Optional[typing.Callable[[pyuavcan.transport.Feedback], None]] = None
        self._injected_exception: typing.Optional[Exception] = None
        self._should_timeout = False

    def enable_feedback(self, handler: typing.Callable[[pyuavcan.transport.Feedback], None]) -> None:
        self._feedback_handler = handler

    def disable_feedback(self) -> None:
        self._feedback_handler = None

    async def send(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        if self._injected_exception is not None:
            raise self._injected_exception

        out = False if self._should_timeout else await self._router(transfer, monotonic_deadline)
        if out:
            self._stats.transfers += 1
            self._stats.frames += 1
            self._stats.payload_bytes += sum(map(len, transfer.fragmented_payload))
            if self._feedback_handler is not None:
                self._feedback_handler(LoopbackFeedback(transfer.timestamp))
        else:
            self._stats.drops += 1

        return out

    @property
    def specifier(self) -> pyuavcan.transport.OutputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
        return self._stats

    def close(self) -> None:
        self._injected_exception = pyuavcan.transport.ResourceClosedError(f"{self} is closed")
        self._closer()

    @property
    def exception(self) -> typing.Optional[Exception]:
        """
        This is a test rigging.
        Use this property to configure an exception object that will be raised when :func:`send` is invoked.
        Set None to remove the injected exception (None is the default value).
        Useful for testing error handling logic.
        """
        return self._injected_exception

    @exception.setter
    def exception(self, value: typing.Optional[Exception]) -> None:
        if isinstance(value, Exception) or value is None:
            self._injected_exception = value
        else:
            raise ValueError(f"Bad exception: {value}")

    @property
    def should_timeout(self) -> bool:
        return self._should_timeout

    @should_timeout.setter
    def should_timeout(self, value: bool) -> None:
        self._should_timeout = bool(value)


def _unittest_session() -> None:
    closed = False

    specifier = pyuavcan.transport.OutputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 123)
    payload_metadata = pyuavcan.transport.PayloadMetadata(1234)

    def do_close() -> None:
        nonlocal closed
        closed = True

    async def do_route(_a: pyuavcan.transport.Transfer, _b: float) -> bool:
        raise NotImplementedError

    ses = LoopbackOutputSession(
        specifier=specifier,
        payload_metadata=payload_metadata,
        loop=asyncio.get_event_loop(),
        closer=do_close,
        router=do_route,
    )

    assert specifier == ses.specifier
    assert payload_metadata == ses.payload_metadata

    assert not closed
    ses.close()
    assert closed

    ts = pyuavcan.transport.Timestamp.now()
    fb = LoopbackFeedback(ts)
    assert fb.first_frame_transmission_timestamp == ts
    assert fb.original_transfer_timestamp == ts
