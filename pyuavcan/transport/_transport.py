#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

from __future__ import annotations
import abc
import typing
import asyncio
import dataclasses
import pyuavcan.util
from ._session import InputSession, OutputSession, InputSessionSpecifier, OutputSessionSpecifier
from ._payload_metadata import PayloadMetadata


@dataclasses.dataclass(frozen=True)
class ProtocolParameters:
    """
    Basic transport capabilities. These parameters are defined by the underlying transport specifications.

    Normally, the values should never change for a particular transport instance.
    This is not a hard guarantee, however.
    For example, a redundant transport aggregator may return a different set of parameters after
    the set of aggregated transports is changed (i.e., a transport is added or removed).
    """

    transfer_id_modulo: int
    """
    The cardinality of the set of distinct transfer-ID values; i.e., the overflow period.
    All high-overhead transports (UDP, Serial, etc.) use a sufficiently large value that will never overflow
    in a realistic, practical scenario.
    The background and motivation are explained at https://forum.uavcan.org/t/alternative-transport-protocols/324.
    Example: 32 for CAN, 72057594037927936 (2**56) for UDP.
    """

    max_nodes: int
    """
    How many nodes can the transport accommodate in a given network.
    Example: 128 for CAN, 4096 for Serial.
    """

    mtu: int
    """
    The maximum number of payload bytes in a single-frame transfer.
    If the number of payload bytes in a transfer exceeds this limit, the transport will spill
    the data into a multi-frame transfer.
    Example: 7 for Classic CAN, <=63 for CAN FD.
    """


@dataclasses.dataclass
class TransportStatistics:
    """
    Base class for transport-specific low-level statistical counters.
    Not to be confused with :class:`pyuavcan.transport.SessionStatistics`,
    which is tracked per-session.
    """
    pass


class Transport(abc.ABC):
    """
    An abstract UAVCAN transport interface. Please read the module documentation for details.

    Implementations should ensure that properties do not raise exceptions.
    """
    @property
    @abc.abstractmethod
    def loop(self) -> asyncio.AbstractEventLoop:
        """
        The asyncio event loop used to operate the transport instance.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def protocol_parameters(self) -> ProtocolParameters:
        """
        Provides information about the properties of the transport protocol implemented by the instance.
        See :class:`ProtocolParameters`.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_node_id(self) -> typing.Optional[int]:
        """
        The node-ID is set once during initialization of the transport,
        either explicitly (e.g., CAN) or by deriving the node-ID value from the configuration
        of the underlying protocol layers (e.g., UDP/IP).

        If the transport does not have a node-ID, this property has the value of None,
        and the transport (and the node that uses it) is said to be in the anonymous mode.
        While in the anonymous mode, some transports may choose to operate in a particular regime to facilitate
        plug-and-play node-ID allocation (for example, a CAN transport may disable automatic retransmission).

        Protip: If you feel like assigning the node-ID after initialization,
        make a proxy that implements this interface and keeps a private transport instance.
        When the node-ID is assigned, the private transport instance is destroyed,
        a new one is implicitly created in its place, and all of the dependent session instances are automatically
        recreated transparently for the user of the proxy.
        This logic is implemented in the redundant transport, which can be used even if no redundancy is needed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        """
        Closes all active sessions, underlying media instances, and other resources related to this transport instance.

        After a transport is closed, none of its methods nor dependent objects (such as sessions) can be used.
        Methods invoked on a closed transport or any of its dependent objects should immediately
        raise :class:`pyuavcan.transport.ResourceClosedError`.
        Subsequent calls to close() will have no effect.

        Failure to close any of the resources does not prevent the method from closing other resources
        (best effort policy).
        Related exceptions may be suppressed and logged; the last occurred exception may be raised after
        all resources are closed if such behavior is considered to be meaningful.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_input_session(self, specifier: InputSessionSpecifier, payload_metadata: PayloadMetadata) -> InputSession:
        """
        This factory method is the only valid way of constructing input session instances.
        Beware that construction and retirement of sessions may be costly.

        The transport will always return the same instance unless there is no session object with the requested
        specifier, in which case it will be created and stored internally until closed.
        The payload metadata parameter is used only when a new instance is created, ignored otherwise.
        Implementations are encouraged to use a covariant return type annotation.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_output_session(self, specifier: OutputSessionSpecifier, payload_metadata: PayloadMetadata) -> OutputSession:
        """
        This factory method is the only valid way of constructing output session instances.
        Beware that construction and retirement of sessions may be costly.

        The transport will always return the same instance unless there is no session object with the requested
        specifier, in which case it will be created and stored internally until closed.
        The payload metadata parameter is used only when a new instance is created, ignored otherwise.
        Implementations are encouraged to use a covariant return type annotation.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def sample_statistics(self) -> TransportStatistics:
        """
        Samples the low-level transport stats.
        The returned object shall be new or cloned (should not refer to an internal field).
        Implementations should annotate the return type as a derived custom type.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def input_sessions(self) -> typing.Sequence[InputSession]:
        """
        Immutable view of all input sessions that are currently open.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def output_sessions(self) -> typing.Sequence[OutputSession]:
        """
        Immutable view of all output sessions that are currently open.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def sniff(self, handler: SnifferCallback) -> None:
        """
        .. warning::
            The advanced network diagnostics API is not yet stable. Be prepared for it to break between minor revisions.
            Suggestions and feedback are welcomed at https://forum.uavcan.org.

        Activates low-level monitoring of the transport interface.

        This method puts the transport instance into the low-level capture mode which does not interfere with its
        normal operation but may significantly increase the computing load due to the need to process every frame
        exchanged over the network (not just frames that originate or terminate at the local node).
        This usually involves reconfiguration of the local networking hardware.
        For instance, the network card may be put into promiscuous mode,
        the CAN adapter will have its acceptance filters disabled, etc.

        The sniffing handler is invoked for every transmitted or received transport frame and, possibly, some
        additional transport-implementation-specific events (e.g., network errors or hardware state changes)
        which are described in the specific transport implementation docs.
        The temporal order of the events delivered to the user may be distorted, depending on the guarantees
        provided by the hardware and its driver.
        This means that if the network hardware sees TX frame A and then RX frame B separated by a very short time
        interval, the user may occasionally see the sequence inverted as (B, A).

        There may be an arbitrary number of sniffing handlers installed; when a new handler is installed, it is
        added to the existing ones, if any.

        If the transport does not support sniffing, this method may have no observable effect.
        Technically, the sniffing protocol, as you can see, does not present any requirements to the emitted events,
        so an implementation that pretends to enter the sniffing mode while not actually doing anything is compliant.

        Since sniffing reflects actual network events, deterministic data loss mitigation will make the sniffer emit
        duplicate frames for affected transfers (although this is probably obvious enough without this elaboration).

        Currently, it is not possible to disable sniffing. Once enabled, it will go on until the transport instance
        is destroyed.

        :param handler: A one-argument callable invoked to inform the user about transport-level events.
            The type of the argument is :class:`Sniff`, see transport-specific docs for the list of the possible
            concrete types and what events they represent.
            The callable may be invoked from a different thread so the user should ensure synchronization.
            If the callable raises an exception, it is suppressed and logged.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _get_repr_fields(self) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
        """
        Returns a list of positional and keyword arguments to :func:`pyuavcan.util.repr_attributes_noexcept`
        for processing the :meth:`__repr__` call.
        The resulting string constructed by repr should resemble a valid Python expression that would yield
        an identical transport instance upon its evaluation.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        """
        Implementations should never override this method. Instead, see :meth:`_get_repr_fields`.
        """
        positional, keyword = self._get_repr_fields()
        return pyuavcan.util.repr_attributes_noexcept(self, *positional, **keyword)


@dataclasses.dataclass(frozen=True)
class Sniff:
    """
    This is the abstract data class for all events reported via the sniffing API.

    If a transport implementation defines multiple event types, it is recommended to define a common superclass
    for them such that it is always possible to determine which transport an event has arrived from using a single
    instance check.
    """
    timestamp: pyuavcan.transport.Timestamp


SnifferCallback = typing.Callable[[Sniff], None]
