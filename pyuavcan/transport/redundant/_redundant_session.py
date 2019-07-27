#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import typing
import logging
import pyuavcan.transport


_logger = logging.getLogger(__name__)


class TransportSpecific(abc.ABC):
    @property
    @abc.abstractmethod
    def transport(self) -> pyuavcan.transport.Transport:
        """
        The transport over which the entity has been or to be transferred.
        """
        raise NotImplementedError


class TransportSpecificFeedback(pyuavcan.transport.Feedback, TransportSpecific):
    @property
    def original_transfer_timestamp(self) -> pyuavcan.transport.Timestamp:
        raise NotImplementedError

    @property
    def first_frame_transmission_timestamp(self) -> pyuavcan.transport.Timestamp:
        raise NotImplementedError

    @property
    def transport(self) -> pyuavcan.transport.Transport:
        raise NotImplementedError


class RedundantSession(abc.ABC):
    @property
    @abc.abstractmethod
    def inferiors(self) -> typing.Sequence[pyuavcan.transport.Session]:
        raise NotImplementedError

    @abc.abstractmethod
    def add_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def remove_inferior(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    def close(self) -> None:
        for s in self.inferiors:
            try:
                s.close()
            except Exception as ex:
                _logger.exception('%s could not close inferior %s: %s', self, s, ex)


class RedundantInputSession(RedundantSession, pyuavcan.transport.InputSession):
    class RedundantTransferFrom(pyuavcan.transport.TransferFrom, TransportSpecific):
        def __init__(self, transport: pyuavcan.transport.Transport):
            self._transport = transport

        @property
        def transport(self) -> pyuavcan.transport.Transport:
            return self._transport

    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def transfer_id_timeout(self) -> float:
        raise NotImplementedError

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        raise NotImplementedError

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        raise NotImplementedError

    def close(self) -> None:
        super(RedundantInputSession, self).close()


class RedundantOutputSession(RedundantSession, pyuavcan.transport.OutputSession):
    def enable_feedback(self, handler: typing.Callable[[TransportSpecificFeedback], None]) -> None:
        raise NotImplementedError

    def disable_feedback(self) -> None:
        raise NotImplementedError

    async def send_until(self, transfer: pyuavcan.transport.Transfer, monotonic_deadline: float) -> bool:
        raise NotImplementedError

    async def send_via_until(self,
                             transfer:           pyuavcan.transport.Transfer,
                             monotonic_deadline: float,
                             transport:          pyuavcan.transport.Transport) -> bool:
        raise NotImplementedError

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        raise NotImplementedError

    def close(self) -> None:
        super(RedundantOutputSession, self).close()
