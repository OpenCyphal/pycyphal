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


class RedundantSession:
    async def add_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    async def remove_transport(self, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError


# ------------------------------------- INPUT -------------------------------------

class RedundantInputSession(RedundantSession, pyuavcan.transport.InputSession):
    pass


class PromiscuousRedundantInput(RedundantInputSession, pyuavcan.transport.PromiscuousInput):
    class RedundantTransferFrom(pyuavcan.transport.TransferFrom, TransportSpecific):
        def __init__(self, transport: pyuavcan.transport.Transport):
            self._transport = transport

        @property
        def transport(self) -> pyuavcan.transport.Transport:
            return self._transport

    @property
    def metadata(self) -> pyuavcan.transport.SessionMetadata:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> RedundantTransferFrom:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[RedundantTransferFrom]:
        raise NotImplementedError


class SelectiveRedundantInput(RedundantInputSession, pyuavcan.transport.SelectiveInput):
    class RedundantTransfer(pyuavcan.transport.Transfer, TransportSpecific):
        def __init__(self, transport: pyuavcan.transport.Transport):
            self._transport = transport

        @property
        def transport(self) -> pyuavcan.transport.Transport:
            return self._transport

    @property
    def metadata(self) -> pyuavcan.transport.SessionMetadata:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def receive(self) -> RedundantTransfer:
        raise NotImplementedError

    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[RedundantTransfer]:
        raise NotImplementedError

    @property
    def source_node_id(self) -> int:
        raise NotImplementedError


# ------------------------------------- OUTPUT -------------------------------------

class RedundantOutputSession(RedundantSession):
    pass


class BroadcastRedundantOutput(RedundantOutputSession, pyuavcan.transport.BroadcastOutput):
    @property
    def metadata(self) -> pyuavcan.transport.SessionMetadata:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    def enable_transmission_timestamping(self, handler: typing.Callable[[TransportSpecificFeedback], None]) -> None:
        raise NotImplementedError

    def disable_transmission_timestamping(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    async def send_via(self, transfer: pyuavcan.transport.Transfer, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError


class UnicastRedundantOutput(RedundantOutputSession, pyuavcan.transport.UnicastOutput):
    @property
    def metadata(self) -> pyuavcan.transport.SessionMetadata:
        raise NotImplementedError

    @property
    def payload_metadata(self) -> pyuavcan.transport.PayloadMetadata:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    def enable_transmission_timestamping(self, handler: typing.Callable[[TransportSpecificFeedback], None]) -> None:
        raise NotImplementedError

    def disable_transmission_timestamping(self) -> None:
        raise NotImplementedError

    async def send(self, transfer: pyuavcan.transport.Transfer) -> None:
        raise NotImplementedError

    async def send_via(self, transfer: pyuavcan.transport.Transfer, transport: pyuavcan.transport.Transport) -> None:
        raise NotImplementedError

    @property
    def destination_node_id(self) -> int:
        raise NotImplementedError
