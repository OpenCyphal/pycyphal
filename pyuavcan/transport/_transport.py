# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

from __future__ import annotations
import abc
import typing
import asyncio
import dataclasses
import pyuavcan.util
from ._session import InputSession, OutputSession, InputSessionSpecifier, OutputSessionSpecifier
from ._payload_metadata import PayloadMetadata
from ._tracer import CaptureCallback, Tracer, AlienTransfer


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
    Example: 32 for CAN, (2**64) for UDP.
    """

    max_nodes: int
    """
    How many nodes can the transport accommodate in a given network.
    Example: 128 for CAN, 65535 for UDP.
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
    def begin_capture(self, handler: CaptureCallback) -> None:
        """
        .. warning::
            This API entity is not yet stable. Suggestions and feedback are welcomed at https://forum.uavcan.org.

        Activates low-level monitoring of the transport interface.
        Also see related method :meth:`make_tracer`.

        This method puts the transport instance into the low-level capture mode which does not interfere with its
        normal operation but may significantly increase the computing load due to the need to process every frame
        exchanged over the network (not just frames that originate or terminate at the local node).
        This usually involves reconfiguration of the local networking hardware.
        For instance, the network card may be put into promiscuous mode,
        the CAN adapter will have its acceptance filters disabled, etc.

        The capture handler is invoked for every transmitted or received transport frame and, possibly, some
        additional transport-implementation-specific events (e.g., network errors or hardware state changes)
        which are described in the specific transport implementation docs.
        The temporal order of the events delivered to the user may be distorted, depending on the guarantees
        provided by the hardware and its driver.
        This means that if the network hardware sees TX frame A and then RX frame B separated by a very short time
        interval, the user may occasionally see the sequence inverted as (B, A).

        There may be an arbitrary number of capture handlers installed; when a new handler is installed, it is
        added to the existing ones, if any.

        If the transport does not support capture, this method may have no observable effect.
        Technically, the capture protocol, as you can see, does not present any requirements to the emitted events,
        so an implementation that pretends to enter the capture mode while not actually doing anything is compliant.

        Since capture reflects actual network events, deterministic data loss mitigation will make the instance emit
        duplicate frames for affected transfers (although this is probably obvious enough without this elaboration).

        It is not possible to disable capture. Once enabled, it will go on until the transport instance is destroyed.

        :param handler: A one-argument callable invoked to inform the user about low-level network events.
            The type of the argument is :class:`Capture`, see transport-specific docs for the list of the possible
            concrete types and what events they represent.
            **The handler may be invoked from a different thread so the user should ensure synchronization.**
            If the handler raises an exception, it is suppressed and logged.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def capture_active(self) -> bool:
        """
        Whether :meth:`begin_capture` was invoked and packet capture is being performed on this transport.
        """
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def make_tracer() -> Tracer:
        """
        .. warning::
            This API entity is not yet stable. Suggestions and feedback are welcomed at https://forum.uavcan.org.

        Use this factory method for constructing tracer implementations for specific transports.
        Concrete tracers may be Voldemort types themselves.
        See also: :class:`Tracer`, :meth:`begin_capture`.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def spoof(self, transfer: AlienTransfer, monotonic_deadline: float) -> bool:
        """
        .. warning::
            This API entity is not yet stable. Suggestions and feedback are welcomed at https://forum.uavcan.org.

        Send a spoofed transfer to the network.
        The configuration of the local transport instance has no effect on spoofed transfers;
        as such, even anonymous instances may send arbitrary spoofed transfers.
        The only relevant property of the instance is which network interface to use for spoofing.

        When this method is invoked for the first time, the transport instance may need to perform one-time
        initialization such as reconfiguring the networking hardware or loading additional drivers.
        Once this one-time initialization is performed,
        the transport instance will reside in the spoofing mode until the instance is closed;
        it is not possible to leave the spoofing mode without closing the instance.
        Some transports/platforms may require special permissions to perform spoofing (esp. IP-based transports).

        If the source node-ID is not provided, an anonymous transfer will be emitted.
        If anonymous transfers are not supported, :class:`pyuavcan.transport.OperationNotDefinedForAnonymousNodeError`
        will be raised.
        Same will happen if one attempted to transmit a multi-frame anonymous transfer.

        If the destination node-ID is not provided, a broadcast transfer will be emitted.
        If the data specifier is that of a service, a :class:`UnsupportedSessionConfigurationError` will be raised.
        The reverse conflict for messages is handled identically.

        Transports with cyclic transfer-ID will compute the modulo automatically.

        This method will update the appropriate statistical counters as usual.

        :returns: True on success, False on timeout.
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
