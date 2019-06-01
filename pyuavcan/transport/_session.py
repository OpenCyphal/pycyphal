#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import dataclasses
from ._transfer import Transfer, TransferFrom
from ._timestamp import Timestamp
from ._data_specifier import DataSpecifier
from ._payload_metadata import PayloadMetadata


class Feedback(abc.ABC):
    @property
    @abc.abstractmethod
    def original_transfer_timestamp(self) -> Timestamp:
        """
        This is the timestamp value supplied when the transfer was created. It can be used by the upper layers
        to match each transmitted transfer with its transmission timestamp.
        Why do we use timestamp for matching? This is because:
            - Priority is rarely unique, hence unfit for matching.
            - Transfer ID may be modified by the transport layer by computing its modulus, which is difficult to
              reliably account for in the application, especially in heterogeneous redundant transports with multiple
              publishers per session.
            - The fragmented payload may contain references to the actual memory of the serialized object, meaning
              that it may actually change after the object is transmitted, also rendering it unfit for matching.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def first_frame_transmission_timestamp(self) -> Timestamp:
        """
        This is the best-effort transmission timestamp. Transport implementations are not required to adhere to
        any specific accuracy goals. They may use either software or hardware timestamping under the hood,
        depending on the capabilities of the underlying media driver.
        The timestamp of a multi-frame transfer equals the timestamp of its first frame.
        The overall stack latency can be computed by subtracting the original transfer timestamp from this value.
        """
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class SessionMetadata:
    data_specifier:   DataSpecifier
    payload_metadata: PayloadMetadata


@dataclasses.dataclass
class Statistics:
    transfers: int = 0
    frames:    int = 0
    bytes:     int = 0
    errors:    int = 0
    overruns:  int = 0


class Session(abc.ABC):
    @property
    @abc.abstractmethod
    def metadata(self) -> SessionMetadata:
        """
        Identifies the category of data exchanged through this session.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def sample_statistics(self) -> Statistics:
        """
        The current approximated statistic sample. We say "approximated" because we do not require the implementations
        to sample the statistical counters atomically, although normally they should strive to do so when possible.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def close(self) -> None:
        """
        After a session is closed, none of its methods can be used anymore. The behavior or methods after close()
        is undefined. Implementations may implement automatic closing from __del__() if possible and appropriate.
        """
        raise NotImplementedError

    def __str__(self) -> str:
        return f'{type(self).__name__}(metadata={self.metadata})'

    def __repr__(self) -> str:
        return self.__str__()


# ------------------------------------- INPUT -------------------------------------

# noinspection PyAbstractClass
class InputSession(Session):
    @abc.abstractmethod
    async def receive(self) -> Transfer:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[Transfer]:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def transfer_id_timeout(self) -> float:
        raise NotImplementedError

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        raise NotImplementedError


# noinspection PyAbstractClass
class PromiscuousInput(InputSession):
    @abc.abstractmethod
    async def receive(self) -> TransferFrom:
        raise NotImplementedError

    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[TransferFrom]:
        raise NotImplementedError


# noinspection PyAbstractClass
class SelectiveInput(InputSession):
    @property
    @abc.abstractmethod
    def source_node_id(self) -> int:
        raise NotImplementedError

    def __str__(self) -> str:
        return f'{type(self).__name__}(metadata={self.metadata}, source_node_id={self.source_node_id})'


# ------------------------------------- OUTPUT -------------------------------------

# noinspection PyAbstractClass
class OutputSession(Session):
    @abc.abstractmethod
    def enable_feedback(self, handler: typing.Callable[[Feedback], None]) -> None:
        """
        Output feedback is disabled by default. It can be enabled by invoking this method. While the feedback is
        enabled, the performance of the transport may be reduced, possibly resulting in higher input/output
        latencies and increased CPU load.

        The transport implementation is allowed to invoke the handler from any context, possibly from another thread.
        The caller should ensure adequate synchronization.

        We avoid full-transfer loopback on purpose because that would make it impossible for us to timestamp outgoing
        transfers independently per transport interface (assuming redundant transports here), since the transport
        aggregation logic would deduplicate redundant received transfers, thus making the valuable timing
        information unavailable.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def disable_feedback(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, transfer: Transfer) -> None:
        raise NotImplementedError


# noinspection PyAbstractClass
class BroadcastOutput(OutputSession):
    pass


# noinspection PyAbstractClass
class UnicastOutput(OutputSession):
    @property
    @abc.abstractmethod
    def destination_node_id(self) -> int:
        raise NotImplementedError

    def __str__(self) -> str:
        return f'{type(self).__name__}(metadata={self.metadata}, destination_node_id={self.destination_node_id})'
