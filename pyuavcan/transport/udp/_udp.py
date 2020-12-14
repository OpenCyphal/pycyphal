#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import copy
import typing
import asyncio
import logging
import ipaddress
import dataclasses
import pyuavcan
from ._session import UDPInputSession, SelectiveUDPInputSession, PromiscuousUDPInputSession
from ._session import UDPOutputSession
from ._frame import UDPFrame
from ._ip import SocketFactory, Sniffer, RawPacket
from ._ip import unicast_ip_to_node_id
from ._socket_reader import SocketReader, SocketReaderStatistics


_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class UDPTransportStatistics(pyuavcan.transport.TransportStatistics):
    received_datagrams: typing.Dict[pyuavcan.transport.DataSpecifier, SocketReaderStatistics] = \
        dataclasses.field(default_factory=dict)
    """
    Basic input session statistics: instances of :class:`SocketReaderStatistics` keyed by data specifier.
    """


class UDPTransport(pyuavcan.transport.Transport):
    """
    The UAVCAN/UDP (IP v4/v6) transport is designed for low-latency, high-throughput, high-reliability
    vehicular networks based on Ethernet.
    Please read the module documentation for details.
    """

    VALID_MTU_RANGE = 1200, 9000
    """
    The minimum is based on the IPv6 specification, which guarantees that the path MTU is at least 1280 bytes large.
    This value is also acceptable for virtually all IPv4 local or real-time networks.
    Lower MTU values shall not be used because they may lead to multi-frame transfer fragmentation where this is
    not expected by the designer, possibly violating the real-time constraints.

    A conventional Ethernet jumbo frame can carry up to 9 KiB (9216 bytes).
    These are the application-level MTU values, so we take overheads into account.
    """

    VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE = (1, 5)

    def __init__(self,
                 local_ip_address:            typing.Union[str, ipaddress.IPv4Address, ipaddress.IPv6Address],
                 *,
                 anonymous:                   bool = False,
                 mtu:                         int = min(VALID_MTU_RANGE),
                 service_transfer_multiplier: int = 1,
                 loop:                        typing.Optional[asyncio.AbstractEventLoop] = None):
        """
        :param local_ip_address: Specifies which local IP address to use for this transport.
            This setting also implicitly specifies the network interface to use.
            All output sockets will be bound (see ``bind()``) to the specified local address.
            If the specified address is not available locally, the transport will fail with
            :class:`pyuavcan.transport.InvalidMediaConfigurationError`.

            For use on the loopback interface, any IP address from the loopback range can be used;
            for example, ``127.0.0.123``.
            This generally does not work with physical interfaces;
            for example, if a host has one physical interface at ``192.168.1.200``,
            an attempt to run a node at ``192.168.1.201`` will trigger the media configuration error
            because ``bind()`` will fail with ``EADDRNOTAVAIL``.
            One can change the node-ID of a physical transport by altering the network
            interface configuration in the underlying operating system itself.

            Using ``INADDR_ANY`` here (i.e., ``0.0.0.0`` for IPv4) is not expected to work reliably or be portable
            because this configuration is, generally, incompatible with multicast sockets (even in the anonymous mode).
            In order to set up even a listening multicast socket, it is necessary to specify the correct local
            address such that the underlying IP stack is aware of which interface to receive multicast packets from.

            When the anonymous mode is enabled, it is quite possible to snoop on the network even if there is
            another node running locally on the same interface
            (because sockets are initialized with ``SO_REUSEADDR`` and ``SO_REUSEPORT``, when available).

        :param anonymous: If True, the transport will reject any attempt to create an output session.
            Additionally, it will report its own local node-ID as None, which is a convention in PyUAVCAN
            to represent anonymous instances.
            The UAVCAN/UDP transport does not support anonymous transfers.

        :param mtu: The application-level MTU for outgoing packets.
            In other words, this is the maximum number of serialized bytes per UAVCAN/UDP frame.
            Transfers where the number of payload bytes does not exceed this value will be single-frame transfers,
            otherwise, multi-frame transfers will be used.
            This setting affects only outgoing frames;
            the MTU of incoming frames is fixed at a sufficiently large value to accept any meaningful UDP frame.

            The default value is the smallest valid value for reasons of compatibility.

        :param service_transfer_multiplier: Deterministic data loss mitigation is disabled by default.
            This parameter specifies the number of times each outgoing service transfer will be repeated.
            This setting does not affect message transfers.

        :param loop: The event loop to use. Defaults to :func:`asyncio.get_event_loop`.
        """
        if not isinstance(local_ip_address, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
            local_ip_address = ipaddress.ip_address(local_ip_address)
        assert not isinstance(local_ip_address, str)
        self._sock_factory = SocketFactory.new(local_ip_address)
        self._anonymous = bool(anonymous)
        self._mtu = int(mtu)
        self._srv_multiplier = int(service_transfer_multiplier)
        self._loop = loop if loop is not None else asyncio.get_event_loop()

        low, high = self.VALID_SERVICE_TRANSFER_MULTIPLIER_RANGE
        if not (low <= self._srv_multiplier <= high):
            raise ValueError(f'Invalid service transfer multiplier: {self._srv_multiplier}')

        low, high = self.VALID_MTU_RANGE
        if not (low <= self._mtu <= high):
            raise ValueError(f'Invalid MTU: {self._mtu} bytes')

        self._socket_reader_registry: typing.Dict[pyuavcan.transport.DataSpecifier, SocketReader] = {}
        self._input_registry: typing.Dict[pyuavcan.transport.InputSessionSpecifier, UDPInputSession] = {}
        self._output_registry: typing.Dict[pyuavcan.transport.OutputSessionSpecifier, UDPOutputSession] = {}

        self._sniffer: typing.Optional[Sniffer] = None
        self._sniffer_handlers: typing.List[pyuavcan.transport.SnifferCallback] = []

        self._closed = False
        self._statistics = UDPTransportStatistics()

        _logger.debug(f'{self}: Initialized with local node-ID {self.local_node_id}')

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def protocol_parameters(self) -> pyuavcan.transport.ProtocolParameters:
        return pyuavcan.transport.ProtocolParameters(
            transfer_id_modulo=UDPFrame.TRANSFER_ID_MASK + 1,
            max_nodes=self._sock_factory.max_nodes,
            mtu=self._mtu,
        )

    @property
    def local_node_id(self) -> typing.Optional[int]:
        addr = self._sock_factory.local_ip_address
        return None if self._anonymous else unicast_ip_to_node_id(addr, addr)

    def close(self) -> None:
        self._closed = True
        for s in (*self.input_sessions, *self.output_sessions):
            try:
                s.close()
            except Exception as ex:  # pragma: no cover
                _logger.exception('%s: Failed to close %r: %s', self, s, ex)
        if self._sniffer is not None:
            self._sniffer.close()
            self._sniffer = None

    def get_input_session(self,
                          specifier:        pyuavcan.transport.InputSessionSpecifier,
                          payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPInputSession:
        self._ensure_not_closed()
        if specifier not in self._input_registry:
            self._setup_input_session(specifier, payload_metadata)
        assert specifier.data_specifier in self._socket_reader_registry
        out = self._input_registry[specifier]
        assert isinstance(out, UDPInputSession)
        assert out.specifier == specifier
        return out

    def get_output_session(self,
                           specifier:        pyuavcan.transport.OutputSessionSpecifier,
                           payload_metadata: pyuavcan.transport.PayloadMetadata) -> UDPOutputSession:
        self._ensure_not_closed()
        if specifier not in self._output_registry:
            if self.local_node_id is None:
                # In UAVCAN/UDP, the anonymous mode is somewhat bolted-on.
                # The underlying protocol (IP) does not have the concept of anonymous packet.
                # We add it artificially as an implementation detail of this library.
                raise pyuavcan.transport.OperationNotDefinedForAnonymousNodeError(
                    'Cannot create an output session instance because this UAVCAN/UDP transport instance is '
                    'configured in the anonymous mode. '
                    'If you need to emit a transfer, create a new instance with anonymous=False.'
                )

            def finalizer() -> None:
                del self._output_registry[specifier]

            multiplier = \
                self._srv_multiplier if isinstance(specifier.data_specifier, pyuavcan.transport.ServiceDataSpecifier) \
                else 1
            sock = self._sock_factory.make_output_socket(specifier.remote_node_id, specifier.data_specifier)
            self._output_registry[specifier] = UDPOutputSession(
                specifier=specifier,
                payload_metadata=payload_metadata,
                mtu=self._mtu,
                multiplier=multiplier,
                sock=sock,
                loop=self._loop,
                finalizer=finalizer,
            )

        out = self._output_registry[specifier]
        assert isinstance(out, UDPOutputSession)
        assert out.specifier == specifier
        return out

    def sniff(self, handler: pyuavcan.transport.SnifferCallback) -> None:
        """
        Reported events are of type :class:`UDPSniff`.

        In order for the network sniffing to work, the local machine should be connected to a SPAN port of the switch.
        See https://en.wikipedia.org/wiki/Port_mirroring and read the documentation for your networking hardware.

        On GNU/Linux, network sniffing requires that either the process is executed by root,
        or the raw packet capture capability ``CAP_NET_RAW`` is enabled.
        For more info read ``man 7 capabilities`` and consider checking the docs for Wireshark/libpcap.

        Packets that do not originate from the current UAVCAN/UDP subnet (configured on this transport instance)
        are not reported via this interface.
        This restriction is critical because there may be other UAVCAN/UDP networks running on the same physical
        L2 network segregated by different subnets, so that if foreign packets were not dropped,
        conflicts would occur.
        """
        self._ensure_not_closed()
        if self._sniffer is None:
            _logger.debug('%s: Starting UDP/IP packet sniffer (hope you have permissions)', self)
            self._sniffer = self._sock_factory.make_sniffer(self._process_sniffed_packet)
        self._sniffer_handlers.append(handler)

    def sample_statistics(self) -> UDPTransportStatistics:
        return copy.copy(self._statistics)

    @property
    def input_sessions(self) -> typing.Sequence[UDPInputSession]:
        return list(self._input_registry.values())

    @property
    def output_sessions(self) -> typing.Sequence[UDPOutputSession]:
        return list(self._output_registry.values())

    @property
    def local_ip_address(self) -> typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]:
        return self._sock_factory.local_ip_address

    def _setup_input_session(self,
                             specifier:        pyuavcan.transport.InputSessionSpecifier,
                             payload_metadata: pyuavcan.transport.PayloadMetadata) -> None:
        """
        In order to set up a new input session, we have to link together a lot of objects. Tricky.
        Also, the setup and teardown actions shall be atomic. Hence the separate method.
        """
        assert specifier not in self._input_registry
        try:
            if specifier.data_specifier not in self._socket_reader_registry:
                _logger.debug('%r: Setting up new socket reader for %s. Existing entries at the moment: %s',
                              self, specifier.data_specifier, self._socket_reader_registry)
                self._socket_reader_registry[specifier.data_specifier] = SocketReader(
                    sock=self._sock_factory.make_input_socket(specifier.data_specifier),
                    local_ip_address=self._sock_factory.local_ip_address,
                    anonymous=self._anonymous,
                    statistics=self._statistics.received_datagrams.setdefault(specifier.data_specifier,
                                                                              SocketReaderStatistics()),
                    loop=self.loop,
                )

            cls: typing.Union[typing.Type[PromiscuousUDPInputSession], typing.Type[SelectiveUDPInputSession]] = \
                PromiscuousUDPInputSession if specifier.is_promiscuous else SelectiveUDPInputSession

            session = cls(specifier=specifier,
                          payload_metadata=payload_metadata,
                          loop=self.loop,
                          finalizer=lambda: self._teardown_input_session(specifier))

            # noinspection PyProtectedMember
            self._socket_reader_registry[specifier.data_specifier].add_listener(specifier.remote_node_id,
                                                                                session._process_frame)
        except Exception:
            self._teardown_input_session(specifier)  # Rollback to ensure atomicity.
            raise

        self._input_registry[specifier] = session

    def _teardown_input_session(self, specifier: pyuavcan.transport.InputSessionSpecifier) -> None:
        """
        The finalizer may be invoked at any point during the setup process,
        so it must be able to deconstruct the pipeline even if it is not fully set up.
        This is why we have these try-except everywhere. Who knew that atomic transactions can be so messy?
        """
        # Unregister the session first.
        try:
            del self._input_registry[specifier]
        except LookupError:
            pass
        # Remove the session from the list of socket reader listeners.
        try:
            demux = self._socket_reader_registry[specifier.data_specifier]
        except LookupError:
            pass    # The reader has not been set up yet, nothing to do.
        else:
            try:
                demux.remove_listener(specifier.remote_node_id)
            except LookupError:
                pass
            # Destroy the reader if there are no listeners left.
            if not demux.has_listeners:
                try:
                    _logger.debug('%r: Destroying %r for %s', self, demux, specifier.data_specifier)
                    demux.close()
                finally:
                    del self._socket_reader_registry[specifier.data_specifier]

    def _process_sniffed_packet(self, timestamp: pyuavcan.transport.Timestamp, packet: RawPacket) -> None:
        """This handler may be invoked from a different thread (the sniffer thread)."""
        pyuavcan.util.broadcast(self._sniffer_handlers)(UDPSniff(timestamp, packet))

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise pyuavcan.transport.ResourceClosedError(f'{self} is closed')

    def _get_repr_fields(self) -> typing.Tuple[typing.List[typing.Any], typing.Dict[str, typing.Any]]:
        return [repr(str(self.local_ip_address))], {
            'anonymous': self._anonymous,
            'service_transfer_multiplier': self._srv_multiplier,
            'mtu': self._mtu,
        }


@dataclasses.dataclass(frozen=True)
class UDPSniff(pyuavcan.transport.Sniff):
    """
    See :meth:`UDPTransport.sniff` for details.
    """
    packet: RawPacket

    def parse(self) -> typing.Optional[typing.Tuple[int,
                                                    typing.Optional[int],
                                                    pyuavcan.transport.DataSpecifier,
                                                    UDPFrame]]:
        """
        A tuple of (source node-ID, destination node-ID (None if broadcast), data specifier, UAVCAN/UDP frame)
        is only defined if the packet is a valid UAVCAN/UDP frame.
        """
        from ._ip import SUBJECT_PORT, udp_port_to_service_data_specifier, multicast_group_to_message_data_specifier

        ip_header = self.packet.ip_header

        dst_nid: typing.Optional[int]
        data_spec: typing.Optional[pyuavcan.transport.DataSpecifier]
        if ip_header.destination.is_multicast:
            if self.packet.udp_header.destination_port != SUBJECT_PORT:
                return None
            dst_nid = None  # Broadcast
            data_spec = multicast_group_to_message_data_specifier(ip_header.source, ip_header.destination)
        else:
            dst_nid = unicast_ip_to_node_id(ip_header.source, ip_header.destination)
            if dst_nid is None:  # The packet crosses the UAVCAN/UDP subnet boundary, invalid.
                return None
            data_spec = udp_port_to_service_data_specifier(self.packet.udp_header.destination_port)

        if data_spec is None:
            return None

        frame = UDPFrame.parse(self.packet.udp_payload)
        if frame is None:
            return None

        src_nid = unicast_ip_to_node_id(ip_header.source, ip_header.source)
        assert src_nid is not None
        return src_nid, dst_nid, data_spec, frame
