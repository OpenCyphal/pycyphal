#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import typing
import pyuavcan.transport


class TransportSpecific(abc.ABC):
    @property
    @abc.abstractmethod
    def transport(self) -> pyuavcan.transport.Transport:
        """The transport over which the entity has been or to be transferred."""
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
    async def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class RedundantInputSession(RedundantSession, pyuavcan.transport.InputSession):
    class RedundantTransferFrom(pyuavcan.transport.TransferFrom, TransportSpecific):
        def __init__(self, transport: pyuavcan.transport.Transport):
            self._transport = transport

        @property
        def transport(self) -> pyuavcan.transport.Transport:
            return self._transport

    async def receive(self) -> RedundantTransferFrom:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
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

    async def close(self) -> None:
        await super(RedundantInputSession, self).close()


class RedundantOutputSession(RedundantSession, pyuavcan.transport.OutputSession):
    def enable_feedback(self, handler: typing.Callable[[TransportSpecificFeedback], None]) -> None:
        raise NotImplementedError

    def disable_feedback(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    async def send_via(self, transfer: pyuavcan.transport.Transfer, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    @property
    def specifier(self) -> pyuavcan.transport.SessionSpecifier:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    def sample_statistics(self) -> pyuavcan.transport.Statistics:
        raise NotImplementedError

    async def close(self) -> None:
        await super(RedundantOutputSession, self).close()
