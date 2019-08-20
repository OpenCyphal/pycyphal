#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import copy
import typing
import asyncio
import logging
import pyuavcan
from .._frame import Frame
from ._base import SerialSession


_logger = logging.getLogger(__name__)


class SerialInputSession(SerialSession, pyuavcan.transport.InputSession):
    #: Units are seconds. Can be overridden after instantiation if needed.
    DEFAULT_TRANSFER_ID_TIMEOUT = 2.0

    def __init__(self,
                 specifier:        pyuavcan.transport.SessionSpecifier,
                 payload_metadata: pyuavcan.transport.PayloadMetadata,
                 loop:             asyncio.AbstractEventLoop,
                 finalizer:        typing.Callable[[], None]):
        """
        Do not call this directly.
        Instead, use the factory method :meth:`pyuavcan.transport.serial.SerialTransport.get_input_session`.
        """
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._loop = loop
        assert self._loop is not None

        self._statistics = pyuavcan.transport.Statistics()
        self._transfer_id_timeout = self.DEFAULT_TRANSFER_ID_TIMEOUT
        self._queue: asyncio.Queue[pyuavcan.transport.TransferFrom] = asyncio.Queue()

        self._reassemblers = [
            pyuavcan.transport.commons.high_overhead_transport.TransferReassembler(nid, payload_metadata.max_size_bytes)
            for nid in Frame.NODE_ID_RANGE
        ]

        super(SerialInputSession, self).__init__(finalizer)

    def _process_frame(self, frame: Frame) -> None:
        """
        This is a part of the transport-internal API. It's a public method despite the name because Python's
        visibility handling capabilities are limited. I guess we could define a private abstract base to
        handle this but it feels like too much work. Why can't we have protected visibility in Python?
        """
        pass

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[pyuavcan.transport.TransferFrom]:
        try:
            timeout = monotonic_deadline - self._loop.time()
            if timeout > 0:
                transfer = await asyncio.wait_for(self._queue.get(), timeout, loop=self._loop)
            else:
                transfer = self._queue.get_nowait()
        except (asyncio.TimeoutError, asyncio.QueueEmpty):
            # If there are unprocessed messages, allow the caller to read them even if the instance is closed.
            self._raise_if_closed()
            return None
        else:
            assert isinstance(transfer, pyuavcan.transport.TransferFrom), 'Internal protocol violation'
            assert transfer.source_node_id in (None, self._specifier.remote_node_id), 'Internal protocol violation'
            return transfer

    @property
    def transfer_id_timeout(self) -> float:
        return self._transfer_id_timeout

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        if value > 0:
            self._transfer_id_timeout = float(value)
        else:
            raise ValueError(f'Invalid value for transfer-ID timeout [second]: {value}')

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        return copy.copy(self._statistics)
