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
from ._session import InputSession, OutputSession, SessionSpecifier
from ._payload_metadata import PayloadMetadata


@dataclasses.dataclass(frozen=True)
class ProtocolParameters:
    """
    Basic transport capabilities. These parameters are defined by a transport specification.

    Normally, the values should never change for a particular transport instance.
    This is not a hard guarantee, however.
    For example, a redundant transport aggregator may return a different set of parameters after
    the set of aggregated transports is changed (e.g., a transport is added or removed).
    """
    transfer_id_modulo:                           int   #: E.g., 32 for CAN, 2**56 for UDP.
    node_id_set_cardinality:                      int   #: E.g., 128 for CAN.
    single_frame_transfer_payload_capacity_bytes: int   #: E.g., 7 for CAN 2.0, <=63 for CAN FD.


@dataclasses.dataclass
class TransportStatistics:
    """
    Base class for transport-specific low-level statistical counters.
    Not to be confused with :class:`pyuavcan.transport.SessionStatistics`,
    which is updated per-session.
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
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def local_node_id(self) -> typing.Optional[int]:
        """
        Generally, the local node ID is not assigned by default, meaning that the local node is in the anonymous mode.
        In that case, the value is None. In general, None is used to represent unspecified node-ID across the library.

        While in the anonymous mode, some transports may choose to operate in a particular mode to facilitate
        plug-and-play node ID allocation. For example, a CAN transport may disable automatic retransmission.

        Some transports, however, may initialize with a node-ID already set if such is dictated by
        the media configuration (for example, a UDP transfer may initialize with the node-ID
        derived from the address of the local network interface).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_local_node_id(self, node_id: int) -> None:
        """
        This method can be invoked only if the local node-ID is not assigned;
        otherwise, :class:`pyuavcan.transport.InvalidTransportConfigurationError` will be raised.
        Once a local node ID is assigned, this method should not be invoked anymore.
        In other words, it can be successfully invoked at most once.
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
    def get_input_session(self, specifier: SessionSpecifier, payload_metadata: PayloadMetadata) -> InputSession:
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
    def get_output_session(self, specifier: SessionSpecifier, payload_metadata: PayloadMetadata) -> OutputSession:
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
        configuration in a human-readable XML-like format. The format is currently very unstable;
        it is probably going to change in the future, so applications should not depend on it yet.

        The returned string shall contain exactly one top-level XML element. The tag name of the element shall match
        the name of the transport class in lower case without the "transport" suffix; e.g:
        ``CANTransport`` -- ``can``, ``SerialTransport`` -- ``serial``.
        The element should contain the name of the OS resource associated with the interface,
        if there is any, e.g., serial port name, network iface name, etc;
        or another element, e.g., further specifying the media layer or similar, which in turn contains the name
        of the associated OS resource in it.
        If it is a pseudo-transport, the element should contain nested elements describing the contained transports,
        if there are any. The attributes of a transport element should contain the values of applicable
        configuration parameters. The charset is ASCII.

        In general, one can view this as an XML-based representation of a Python constructor invocation expression,
        where the first argument is represented as the XML element data, and all following arguments
        are represented as named XML attributes.
        This is not a hard requirement though. Examples:
        ``<can><socketcan mtu="64">vcan0</socketcan></can>``,
        ``<serial baudrate="115200">/dev/ttyACM0</serial>``,
        ``<ieee802154><xbee>/dev/ttyACM0</xbee></ieee802154>``,
        ``<redundant><can><socketcan mtu="8">can0</socketcan></can><serial baudrate="115200">COM9</serial></redundant>``

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
