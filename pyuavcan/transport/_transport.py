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

    @property
    @abc.abstractmethod
    def descriptor(self) -> str:
        """
        A transport-specific specification string containing sufficient information to recreate the current
        configuration in a human-readable XML-like format.

        The returned string shall contain exactly one top-level XML element. The tag name of the element shall match
        the name of the transport class in lower case without the "transport" suffix; e.g:
        ``CANTransport`` -- ``can``, ``SerialTransport`` -- ``serial``.
        The element should contain the name of the OS resource associated with the interface,
        if there is any, e.g., serial port name, network iface name, etc;
        or another element, e.g., further specifying the media layer or similar, which in turn contains the name
        of the associated OS resource in it.
        If it is a pseudo-transport, the element should contain nested elements describing the contained transports,
        if there are any.
        The attributes of a transport element should represent the values of applicable configuration parameters,
        excepting those that are already exposed via :attr:`protocol_parameters` to avoid redundancy.
        The charset is ASCII.

        In general, one can view this as an XML-based representation of a Python constructor invocation expression,
        where the first argument is represented as the XML element data, and all following arguments
        are represented as named XML attributes.
        Examples:

        - ``<can><socketcan mtu="64">vcan0</socketcan></can>``
        - ``<serial baudrate="115200">/dev/ttyACM0</serial>``
        - ``<ieee802154><xbee>/dev/ttyACM0</xbee></ieee802154>``
        - ``<redundant><udp srv_mult="1">127.0.0.42/8</udp><serial baudrate="115200">COM9</serial></redundant>``

        We should consider defining a reverse static factory method that attempts to locate the necessary transport
        implementation class and instantiate it from a supplied descriptor. This would benefit transport-agnostic
        applications greatly.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        """
        Implementations are advised to avoid overriding this method.
        """
        return pyuavcan.util.repr_attributes(self, self.descriptor, self.protocol_parameters,
                                             local_node_id=self.local_node_id)
