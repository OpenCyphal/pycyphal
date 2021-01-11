# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import typing
import asyncio

import pyuavcan.transport


class LoopbackInputSession(pyuavcan.transport.InputSession):
    DEFAULT_TRANSFER_ID_TIMEOUT = 2

    def __init__(
        self,
        specifier: pyuavcan.transport.InputSessionSpecifier,
        payload_metadata: pyuavcan.transport.PayloadMetadata,
        loop: asyncio.AbstractEventLoop,
        closer: typing.Callable[[], None],
    ):
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        self._closer = closer
        self._transfer_id_timeout = float(self.DEFAULT_TRANSFER_ID_TIMEOUT)
        self._stats = pyuavcan.transport.SessionStatistics()
        self._queue: asyncio.Queue[pyuavcan.transport.TransferFrom] = asyncio.Queue()
        super().__init__()

    async def receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        timeout = monotonic_deadline - self._loop.time()
        try:
            if timeout > 0:
                out = await asyncio.wait_for(self._queue.get(), timeout)
            else:
                out = self._queue.get_nowait()
        except asyncio.TimeoutError:
            return None
        except asyncio.QueueEmpty:
            return None
        else:
            self._stats.transfers += 1
            self._stats.frames += 1
            self._stats.payload_bytes += sum(map(len, out.fragmented_payload))
            return out

    async def push(self, transfer: pyuavcan.transport.TransferFrom) -> None:
        """
        Inserts a transfer into the receive queue of this loopback session.
        """
        # TODO: handle Transfer ID like a real transport would: drop duplicates, handle transfer-ID timeout.
        # This is not very important for this demo transport but users may expect a more accurate modeling.
        await self._queue.put(transfer)

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        value = float(value)
        if value > 0:
            self._transfer_id_timeout = float(value)
        else:
            raise ValueError(f"Invalid TID timeout: {value!r}")

    @property
    def specifier(self) -> pyuavcan.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
        return self._stats

    def close(self) -> None:
        self._closer()


def _unittest_session() -> None:
    import pytest

    closed = False

    specifier = pyuavcan.transport.InputSessionSpecifier(pyuavcan.transport.MessageDataSpecifier(123), 123)
    payload_metadata = pyuavcan.transport.PayloadMetadata(1234)

    def do_close() -> None:
        nonlocal closed
        closed = True

    ses = LoopbackInputSession(
        specifier=specifier, payload_metadata=payload_metadata, loop=asyncio.get_event_loop(), closer=do_close
    )

    ses.transfer_id_timeout = 123.456
    with pytest.raises(ValueError):
        ses.transfer_id_timeout = -0.1
    assert ses.transfer_id_timeout == pytest.approx(123.456)

    assert specifier == ses.specifier
    assert payload_metadata == ses.payload_metadata

    assert not closed
    ses.close()
    assert closed
