#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import time
import typing
import asyncio
import collections
import pyuavcan.util
import pyuavcan.transport
from .. import _frame, _can_id
from . import _base, _transfer_receiver


InputQueueItem = typing.Tuple[_can_id.CANID, _frame.TimestampedUAVCANFrame]


class InputSession(_base.Session):
    DEFAULT_TRANSFER_ID_TIMEOUT = 2     # [second] Per the Specification.

    def __init__(self,
                 data_specifier: pyuavcan.transport.DataSpecifier,
                 loop:           typing.Optional[asyncio.AbstractEventLoop],
                 queue:          asyncio.Queue[InputQueueItem],
                 finalizer:      _base.Finalizer):
        self._data_specifier = data_specifier
        self._queue = queue
        self._lock = asyncio.Lock(loop=loop)
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._transfer_id_timeout_ns = int(InputSession.DEFAULT_TRANSFER_ID_TIMEOUT / _NANO)

        self._receivers = [_transfer_receiver.TransferReceiver(data_specifier.max_payload_size_bytes)
                           for _ in _node_id_range()]

        self._success_count_anonymous = 0
        self._success_count = [0 for _ in _node_id_range()]
        self._error_counts: typing.List[typing.DefaultDict[_transfer_receiver.TransferReceptionError, int]] = [
            collections.defaultdict(int) for _ in _node_id_range()
        ]

        super(InputSession, self).__init__(finalizer=finalizer)

    async def _do_try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        async with self._lock:
            while True:
                timeout = monotonic_deadline - time.monotonic()
                if timeout <= 0:
                    break

                canid, frame = await asyncio.wait_for(self._queue.get(), timeout, loop=self._loop)
                assert isinstance(canid, _can_id.CANID), 'Internal session protocol violation'
                assert isinstance(frame, _frame.TimestampedUAVCANFrame), 'Internal session protocol violation'

                if isinstance(canid, _can_id.MessageCANID):
                    assert isinstance(self._data_specifier, pyuavcan.transport.MessageDataSpecifier)
                    assert self._data_specifier.subject_id == canid.subject_id
                    source_node_id = canid.source_node_id
                    if source_node_id is None:
                        # Anonymous transfer - no reconstruction needed
                        self._success_count_anonymous += 1
                        return pyuavcan.transport.TransferFrom(timestamp=frame.timestamp,
                                                               priority=canid.priority,
                                                               transfer_id=frame.transfer_id,
                                                               fragmented_payload=[frame.padded_payload],
                                                               source_node_id=None)

                elif isinstance(canid, _can_id.ServiceCANID):
                    assert isinstance(self._data_specifier, pyuavcan.transport.ServiceDataSpecifier)
                    assert self._data_specifier.service_id == canid.service_id
                    assert (self._data_specifier.role == self._data_specifier.Role.SERVER) == canid.request_not_response
                    source_node_id = canid.source_node_id

                else:
                    assert False

                receiver = self._receivers[source_node_id]
                result = receiver.process_frame(canid.priority, source_node_id, frame, self._transfer_id_timeout_ns)
                if isinstance(result, _transfer_receiver.TransferReceptionError):
                    self._error_counts[source_node_id][result] += 1
                elif isinstance(result, pyuavcan.transport.TransferFrom):
                    self._success_count[source_node_id] += 1
                    return result
                elif result is None:
                    pass        # Nothing to do - expecting more frames
                else:
                    assert False

        return None


class PromiscuousInputSession(InputSession, pyuavcan.transport.PromiscuousInputSession):
    def __init__(self,
                 data_specifier: pyuavcan.transport.DataSpecifier,
                 loop:           typing.Optional[asyncio.AbstractEventLoop],
                 queue:          asyncio.Queue[InputQueueItem],
                 finalizer:      _base.Finalizer):
        super(PromiscuousInputSession, self).__init__(data_specifier=data_specifier,
                                                      loop=loop,
                                                      queue=queue,
                                                      finalizer=finalizer)

    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        return self._data_specifier

    async def close(self) -> None:
        self._finalizer()

    async def receive(self) -> pyuavcan.transport.TransferFrom:
        out: typing.Optional[pyuavcan.transport.TransferFrom] = None
        while out is None:
            out = await self.try_receive(time.monotonic() + _INFINITE_RECEIVE_RETRY_INTERVAL)
        return out

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        return await self._do_try_receive(monotonic_deadline)

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout_ns * _NANO

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        self._transfer_id_timeout_ns = int(value / _NANO)

    @property
    def error_counts_per_source_node_id(self) -> \
            typing.Dict[int, typing.DefaultDict[_transfer_receiver.TransferReceptionError, int]]:
        return {nid: self._error_counts[nid].copy() for nid in _node_id_range()}

    @property
    def transfer_count_per_source_node_id(self) -> typing.Dict[int, int]:
        return {nid: self._success_count[nid] for nid in _node_id_range()}

    @property
    def anonymous_transfer_count(self) -> int:
        return self._success_count_anonymous


class SelectiveInputSession(InputSession, pyuavcan.transport.SelectiveInputSession):
    def __init__(self,
                 source_node_id: int,
                 data_specifier: pyuavcan.transport.DataSpecifier,
                 loop:           typing.Optional[asyncio.AbstractEventLoop],
                 queue:          asyncio.Queue[InputQueueItem],
                 finalizer:      _base.Finalizer):
        self._source_node_id = int(source_node_id)
        super(SelectiveInputSession, self).__init__(data_specifier=data_specifier,
                                                    loop=loop,
                                                    queue=queue,
                                                    finalizer=finalizer)

    @property
    def data_specifier(self) -> pyuavcan.transport.DataSpecifier:
        return self._data_specifier

    async def close(self) -> None:
        self._finalizer()

    async def receive(self) -> pyuavcan.transport.Transfer:
        out: typing.Optional[pyuavcan.transport.Transfer] = None
        while out is None:
            out = await self.try_receive(time.monotonic() + _INFINITE_RECEIVE_RETRY_INTERVAL)
        return out

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.Transfer]:
        out = await self._do_try_receive(monotonic_deadline)
        assert out is None or out.source_node_id == self.source_node_id, 'Mishandled selective input session'
        return out

    @property
    def source_node_id(self) -> int:
        return self._source_node_id

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout_ns * _NANO

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        self._transfer_id_timeout_ns = int(value / _NANO)

    @property
    def error_counts(self) -> typing.DefaultDict[_transfer_receiver.TransferReceptionError, int]:
        return self._error_counts[self.source_node_id].copy()

    @property
    def transfer_count(self) -> int:
        return self._success_count[self.source_node_id]


def _node_id_range() -> typing.Iterable[int]:
    return range(_can_id.CANID.NODE_ID_MASK + 1)


_NANO = 1e-9

_INFINITE_RECEIVE_RETRY_INTERVAL = 60
