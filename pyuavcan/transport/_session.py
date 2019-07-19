#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import dataclasses
import pyuavcan.util
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
class SessionSpecifier:
    """
    This is like a regular session specifier except that we assume that one end of a session terminates at the
    local node. If the remote node ID is not set, then for output sessions this implies broadcast and for input
    sessions this implies promiscuity. If set, then output sessions will be unicast to that node ID and input
    sessions will ignore all transfers except those originating from the specified node ID.
    """
    data_specifier: DataSpecifier
    remote_node_id: typing.Optional[int]

    def __post_init__(self) -> None:
        if self.remote_node_id is not None and self.remote_node_id < 0:
            raise ValueError(f'Invalid remote node ID: {self.remote_node_id}')


@dataclasses.dataclass
class Statistics:
    transfers:     int = 0  # Number of UAVCAN transfers
    frames:        int = 0  # Number of UAVCAN frames
    payload_bytes: int = 0  # Number of transport layer payload bytes, i.e., not including transport metadata or padding
    errors:        int = 0  # Number of failures of any kind, even if they are also logged using other means
    overruns:      int = 0  # Number of buffer overruns

    def __eq__(self, other: object) -> bool:
        """
        The statistic comparison operator is defined for any combination of derived classes. It compares only
        those fields that are available in both operands, ignoring unique fields. This is useful for testing.
        """
        if isinstance(other, Statistics):
            fds = set(f.name for f in dataclasses.fields(self)) & set(f.name for f in dataclasses.fields(other))
            return all(getattr(self, n) == getattr(other, n) for n in fds)
        else:  # pragma: no cover
            return NotImplemented


class Session(abc.ABC):
    """
    Properties should not raise exceptions.
    """

    @property
    @abc.abstractmethod
    def specifier(self) -> SessionSpecifier:
        """
        Data specifier plus the remote node ID. The latter is optional.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def payload_metadata(self) -> PayloadMetadata:
        """
        Identifies the properties of the payload exchanged through this session.
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
    def close(self) -> None:
        """
        After a session is closed, none of its methods can be used. The behavior of methods invoked on a closed
        session is undefined, unless explicitly documented otherwise; subsequent calls to close() will have no effect.

        Methods where a task is blocked (such as receive()) at the time of close() will raise a
        :class:`pyuavcan.transport.ResourceClosedError` upon next invocation or sooner.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, specifier=self.specifier, payload_metadata=self.payload_metadata)


# noinspection PyAbstractClass
class InputSession(Session):
    @abc.abstractmethod
    async def try_receive(self, monotonic_deadline: float) -> typing.Optional[TransferFrom]:
        """
        Return None if the transfer is not received before the deadline [second].
        The deadline is compared against time.monotonic().
        If a transfer is received before the deadline, behaves like the non-timeout-capable version.
        If the deadline is in the past, checks once if there is a transfer and then returns immediately, either the
        transfer or None if there is none.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def transfer_id_timeout(self) -> float:
        """
        By default, the transfer ID timeout is initialized with the default value provided in the Specification.
        It can be overridden using this interface if necessary (rarely is). The units are seconds.
        """
        raise NotImplementedError

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        """
        Raises ValueError if the value is not positive.
        """
        raise NotImplementedError

    @property
    def source_node_id(self) -> typing.Optional[int]:
        return self.specifier.remote_node_id


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
        """
        Restores the original state.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, transfer: Transfer) -> None:
        """
        May throw SendTimeoutError.
        """
        raise NotImplementedError

    @property
    def destination_node_id(self) -> typing.Optional[int]:
        return self.specifier.remote_node_id
