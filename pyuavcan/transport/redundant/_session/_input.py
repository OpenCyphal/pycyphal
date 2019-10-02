#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import typing
import asyncio
import logging
import pyuavcan.transport
from ._base import RedundantSession


_logger = logging.getLogger(__name__)


class RedundantTransferFrom(pyuavcan.transport.TransferFrom):
    def __init__(self, transport: pyuavcan.transport.Transport):
        self._transport = transport

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        return self._transport


class RedundantInputSession(RedundantSession, pyuavcan.transport.InputSession):
    """

    """
    def __init__(self,
                 specifier:                 pyuavcan.transport.InputSessionSpecifier,
                 payload_metadata:          pyuavcan.transport.PayloadMetadata,
                 tid_monotonicity_provider: typing.Callable[[], bool],
                 loop_provider:             typing.Callable[[], asyncio.AbstractEventLoop],
                 finalizer:                 typing.Callable[[], None]):
        self._specifier = specifier
        self._payload_metadata = payload_metadata
        self._is_tid_monotonic = tid_monotonicity_provider
        self._loop_provider = loop_provider
        self._finalizer: typing.Optional[typing.Callable[[], None]] = finalizer
        assert isinstance(self._specifier, pyuavcan.transport.InputSessionSpecifier)
        assert isinstance(self._payload_metadata, pyuavcan.transport.PayloadMetadata)
        assert isinstance(self._is_tid_monotonic(), bool)
        assert isinstance(self._loop_provider(), asyncio.AbstractEventLoop)
        assert callable(self._finalizer)

        self._inferiors: typing.List[pyuavcan.transport.InputSession] = []
        self._tid_timeout: typing.Optional[float] = None

        self._stat_transfers = 0
        self._stat_payload_bytes = 0
        self._stat_errors = 0

    def _add_inferior(self, session: pyuavcan.transport.Session) -> None:
        assert isinstance(session, pyuavcan.transport.InputSession), 'Internal error'
        assert self._finalizer is not None, 'Internal logic error: the session was supposed to be unregistered'
        assert session.specifier == self.specifier, 'Internal error'
        assert session.payload_metadata == self.payload_metadata, 'Internal error'
        if session not in self._inferiors:
            if self._tid_timeout is not None:
                session.transfer_id_timeout = self._tid_timeout
            # If and only if all went well, add the new inferior to the set.
            self._inferiors.append(session)

    def _close_inferior(self, session: pyuavcan.transport.Session) -> None:
        assert isinstance(session, pyuavcan.transport.InputSession), 'Internal error'
        assert self._finalizer is not None, 'Internal logic error: the session was supposed to be unregistered'
        assert session.specifier == self.specifier, 'Internal error'
        assert session.payload_metadata == self.payload_metadata, 'Internal error'
        try:
            self._inferiors.remove(session)
        except ValueError:
            pass
        else:
            session.close()  # May raise.

    @property
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.InputSession]:
        return self._inferiors

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
        raise NotImplementedError

    @property
    def transfer_id_timeout(self) -> float:
        if self._inferiors:
            return self._inferiors[0].transfer_id_timeout
        elif self._tid_timeout is not None:
            return self._tid_timeout
        else:
            return 0.0  # This indicates that no preference has been assigned. Clumsy.

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        value = float(value)
        if value <= 0.0:
            raise ValueError(f'Transfer-ID timeout shall be a positive number of seconds, got {value}')
        self._tid_timeout = value
        for s in self._inferiors:
            s.transfer_id_timeout = value

    @property
    def specifier(self) -> pyuavcan.transport.InputSessionSpecifier:
        return self._specifier

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        return self._payload_metadata

    def sample_statistics(self) -> pyuavcan.transport.SessionStatistics:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
