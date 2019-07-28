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
    """
    Abstract output transfer feedback for transmission timestamping.
    If feedback is enabled for an output session, an instance of this class is delivered back to the application
    via a callback soon after the first frame of the transfer is emitted.

    The upper layers can match a feedback object with its transfer by the transfer creation timestamp.
    """

    @property
    @abc.abstractmethod
    def original_transfer_timestamp(self) -> Timestamp:
        """
        This is the timestamp value of the original outgoing transfer object;
        normally it is the transfer creation timestamp.
        This value can be used by the upper layers to match each transmitted transfer with its transmission timestamp.
        Why do we use timestamp for matching? This is because:

        - The priority is rarely unique, hence unfit for matching.

        - Transfer-ID may be modified by the transport layer by computing its modulus, which is difficult to
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
        This is the best-effort estimate of the transmission timestamp.
        Transport implementations are not required to adhere to any specific accuracy goals.
        They may use either software or hardware timestamping under the hood,
        depending on the capabilities of the underlying media driver.
        The timestamp of a multi-frame transfer is the timestamp of its first frame.
        The overall stack latency can be computed by subtracting the original transfer timestamp from this value.
        """
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class SessionSpecifier:
    """
    This is like a regular session specifier (https://forum.uavcan.org/t/alternative-transport-protocols/324)
    except that we assume that one end of a session terminates at the local node.

    If the remote node-ID is not set, then for output sessions this implies broadcast and for input
    sessions this implies promiscuity.
    If it is set, then output sessions will be unicast to that node-ID and input sessions will ignore
    all transfers except those originating from the specified node-ID.
    """
    #: See :class:`pyuavcan.transport.DataSpecifier`.
    data_specifier: DataSpecifier

    #: If not None: output sessions are unicast to that node-ID, and input sessions ignore all transfers
    #: except those that originate from the specified remote node-ID.
    #: If None: output sessions are broadcast and input sessions are promiscuous.
    remote_node_id: typing.Optional[int]

    def __post_init__(self) -> None:
        if self.remote_node_id is not None and self.remote_node_id < 0:
            raise ValueError(f'Invalid remote node-ID: {self.remote_node_id}')


@dataclasses.dataclass
class Statistics:
    """
    Abstract transport-agnostic session statistics.
    Transport implementations are encouraged to extend this class to add more transport-specific information.
    The statistical counters start from zero when a session is first instantiated.
    """
    transfers:     int = 0  #: UAVCAN transfer count.
    frames:        int = 0  #: UAVCAN transport frame count (CAN frames, UDP packets, wireless frames, etc).
    payload_bytes: int = 0  #: Transport layer payload bytes, i.e., not including transport metadata or padding.
    errors:        int = 0  #: Failures of any kind, even if they are also logged using other means.
    drops:         int = 0  #: Frames lost to buffer overruns and expired deadlines.

    def __eq__(self, other: object) -> bool:
        """
        The statistic comparison operator is defined for any combination of derived classes.
        It compares only those fields that are available in both operands, ignoring unique fields.
        This is useful for testing.
        """
        if isinstance(other, Statistics):
            fds = set(f.name for f in dataclasses.fields(self)) & set(f.name for f in dataclasses.fields(other))
            return all(getattr(self, n) == getattr(other, n) for n in fds)
        else:  # pragma: no cover
            return NotImplemented


class Session(abc.ABC):
    """
    Abstract session base class. This is further specialized by input and output.
    Properties should not raise exceptions.
    """

    @property
    @abc.abstractmethod
    def specifier(self) -> SessionSpecifier:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def payload_metadata(self) -> PayloadMetadata:
        raise NotImplementedError

    @abc.abstractmethod
    def sample_statistics(self) -> Statistics:
        """
        Samples and returns the approximated statistics.
        We say "approximated" because implementations are not required to sample the counters atomically,
        although normally they should strive to do so when possible.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """
        After a session is closed, none of its methods can be used.
        Methods invoked on a closed session should immediately raise :class:`pyuavcan.transport.ResourceClosedError`.
        Subsequent calls to close() will have no effect (no exception either).

        Methods where a task is blocked (such as receive()) at the time of close() will raise a
        :class:`pyuavcan.transport.ResourceClosedError` upon next invocation or sooner.
        Callers of such blocking methods are recommended to avoid usage of large timeouts to facilitate
        faster reaction to transport closure.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, specifier=self.specifier, payload_metadata=self.payload_metadata)


# noinspection PyAbstractClass
class InputSession(Session):
    """
    Either promiscuous or selective input session.
    The configuration cannot be changed once instantiated.

    Users shall never construct instances themselves;
    instead, the factory method :meth:`pyuavcan.transport.Transport.get_input_session` shall be used.
    """
    @abc.abstractmethod
    async def receive_until(self, monotonic_deadline: float) -> typing.Optional[TransferFrom]:
        """
        Attempts to receive the transfer before the deadline [second].
        Returns None if the transfer is not received before the deadline.
        The deadline is compared against :meth:`asyncio.AbstractEventLoop.time`.
        If the deadline is in the past, checks once if there is a transfer and then returns immediately
        without context switching.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def transfer_id_timeout(self) -> float:
        """
        By default, the transfer-ID timeout [second] is initialized with the default value provided in the
        UAVCAN specification.
        It can be overridden using this interface if necessary (rarely is).
        An attempt to assign an invalid timestamp value raises :class:`ValueError`.
        """
        raise NotImplementedError

    @transfer_id_timeout.setter
    def transfer_id_timeout(self, value: float) -> None:
        raise NotImplementedError

    @property
    def source_node_id(self) -> typing.Optional[int]:
        """
        Alias for ``.specifier.remote_node_id``.
        For promiscuous sessions this is always None.
        For selective sessions this is the node-ID of the source.
        """
        return self.specifier.remote_node_id


# noinspection PyAbstractClass
class OutputSession(Session):
    """
    Either broadcast or unicast output session.
    The configuration cannot be changed once instantiated.

    Users shall never construct instances themselves;
    instead, the factory method :meth:`pyuavcan.transport.Transport.get_output_session` shall be used.
    """
    @abc.abstractmethod
    def enable_feedback(self, handler: typing.Callable[[Feedback], None]) -> None:
        """
        The output feedback feature makes the transport invoke the specified handler soon after the first
        frame of each transfer originating from this session instance is delivered to the network interface
        or similar underlying logic (not to be confused with delivery to the destination node!).
        This is designed for transmission timestamping, which in turn is necessary for certain protocol features
        such as highly accurate time synchronization.

        The handler is invoked with one argument of type :class:`pyuavcan.transport.Feedback`
        which contains the timing information.
        The transport implementation is allowed to invoke the handler from any context, possibly from another thread.
        The caller should ensure adequate synchronization.
        The actual delay between the emission of the first frame and invocation of the callback is
        implementation-defined, but implementations should strive to minimize it.

        Output feedback is disabled by default. It can be enabled by invoking this method.
        While the feedback is enabled, the performance of the transport in general (not just this session instance)
        may be reduced, possibly resulting in higher input/output latencies and increased CPU load.

        When feedback is already enabled at the time of invocation, this method removes the old callback
        and installs the new one instead.

        Design motivation: We avoid full-transfer loopback such as used in Libuavcan (at least in its old version)
        on purpose because that would make it impossible for us to timestamp outgoing transfers independently
        per transport interface (assuming redundant transports here), since the transport aggregation logic
        would deduplicate redundant received transfers, thus making the valuable timing information unavailable.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def disable_feedback(self) -> None:
        """
        Restores the original state.
        Does nothing if the callback is already disabled.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def send_until(self, transfer: Transfer, monotonic_deadline: float) -> bool:
        """
        Sends the transfer; block if necessary until the specified deadline [second].
        Return when transmission is completed, in which case the return value is True;
        or return when the deadline is reached, in which case the return value is False.
        In the case of timeout, a multi-frame transfer may be emitted partially,
        thereby rendering the receiving end unable to process it.
        If the deadline is in the past, the method attempts to send the frames anyway as long as that
        doesn't involve blocking (i.e., task context switching).

        Some transports or media sub-layers may be unable to guarantee transmission strictly before the deadline;
        for example, that may be the case if there is an additional buffering layer under the transport/media
        implementation (e.g., that could be the case with SLCAN-interfaced CAN bus adapters, IEEE 802.15.4 radios,
        and so on, where the data is pushed through an intermediary interface and briefly buffered again before
        being pushed onto the media).
        This is a design limitation imposed by the underlying non-real-time platform that Python runs on;
        it is considered acceptable since PyUAVCAN is designed for soft-real-time applications at most.
        """
        raise NotImplementedError

    @property
    def destination_node_id(self) -> typing.Optional[int]:
        """
        Alias for ``.specifier.remote_node_id``.
        For broadcast sessions this is always None.
        For unicast sessions this is the node-ID of the destination.
        """
        return self.specifier.remote_node_id
